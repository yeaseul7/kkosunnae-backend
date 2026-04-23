import {Pinecone} from "@pinecone-database/pinecone";
import {
  fetchAbandonmentApiPage,
  parseShelterItemList,
  SHELTER_API_PAGE_SIZE,
} from "../shared/shelterAnimalFirestoreWrite.js";
import {PINECONE_INDEX_NAME} from "../shared/imageEmbedding.js";

const PINECONE_LIST_PAGE_SIZE = 99;
const PINECONE_DELETE_BATCH_SIZE = 1000;

export interface CleanupPineconeEmbeddingsOptions {
  dryRun?: boolean;
}

export interface CleanupPineconeEmbeddingsResult {
  ok: true;
  dryRun: boolean;
  apiRows: number;
  apiIds: number;
  pineconeIds: number;
  staleIds: number;
  deleted: number;
}

/**
 * 공공데이터 API 전체 페이지를 순회해 현재 유효한 Pinecone record id 목록 생성
 * @param {string} serviceKey 공공데이터 API 키
 * @return {Promise<object>} API row 수와 현재 id set
 */
async function fetchCurrentAnimalIds(
  serviceKey: string
): Promise<{apiRows: number; ids: Set<string>}> {
  const ids = new Set<string>();
  let apiRows = 0;
  let pageNo = 1;
  let totalPages = 1;

  while (pageNo <= totalPages) {
    const data = await fetchAbandonmentApiPage(
      serviceKey,
      pageNo,
      SHELTER_API_PAGE_SIZE
    );
    const {itemList, totalCount} = parseShelterItemList(data);
    apiRows += itemList.length;
    totalPages = Math.max(1, Math.ceil(totalCount / SHELTER_API_PAGE_SIZE));

    for (const item of itemList) {
      const desertionNo = String(item.desertionNo ?? "").trim();
      const careRegNo = String(item.careRegNo ?? "").trim();
      if (!desertionNo || !careRegNo) continue;
      ids.add(`${desertionNo}-${careRegNo}`);
    }

    console.info(
      `Pinecone 정리 기준 API 조회: page ${pageNo}/${totalPages}, ` +
        `누적 id ${ids.size}건`
    );
    pageNo++;
  }

  return {apiRows, ids};
}

/**
 * Pinecone 인덱스의 전체 record id 목록 조회
 * @param {string} pineconeApiKey Pinecone API 키
 * @return {Promise<string[]>} Pinecone record id 목록
 */
async function listPineconeIds(pineconeApiKey: string): Promise<string[]> {
  const pc = new Pinecone({apiKey: pineconeApiKey});
  const index = pc.index(PINECONE_INDEX_NAME);
  const ids: string[] = [];
  let paginationToken: string | undefined;

  do {
    const response = await index.listPaginated({
      limit: PINECONE_LIST_PAGE_SIZE,
      paginationToken,
    });
    for (const vector of response.vectors ?? []) {
      if (vector.id) ids.push(vector.id);
    }
    paginationToken = response.pagination?.next;
    console.info(`Pinecone id 조회 진행: 누적 ${ids.length}건`);
  } while (paginationToken);

  return ids;
}

/**
 * 공공데이터에 없는 Pinecone record 삭제
 * @param {string} serviceKey 공공데이터 API 키
 * @param {string} pineconeApiKey Pinecone API 키
 * @param {CleanupPineconeEmbeddingsOptions} options 실행 옵션
 * @return {Promise<CleanupPineconeEmbeddingsResult>} 정리 결과
 */
export async function cleanupPineconeEmbeddings(
  serviceKey: string,
  pineconeApiKey: string,
  options: CleanupPineconeEmbeddingsOptions = {}
): Promise<CleanupPineconeEmbeddingsResult> {
  const dryRun = options.dryRun ?? false;
  const current = await fetchCurrentAnimalIds(serviceKey);
  const pineconeIds = await listPineconeIds(pineconeApiKey);
  const staleIds = pineconeIds.filter((id) => !current.ids.has(id));

  let deleted = 0;
  if (!dryRun && staleIds.length > 0) {
    const pc = new Pinecone({apiKey: pineconeApiKey});
    const index = pc.index(PINECONE_INDEX_NAME);

    for (let i = 0; i < staleIds.length; i += PINECONE_DELETE_BATCH_SIZE) {
      const ids = staleIds.slice(i, i + PINECONE_DELETE_BATCH_SIZE);
      await index.deleteMany({ids});
      deleted += ids.length;
      console.info(
        `Pinecone stale vector 삭제 진행: ${deleted}/${staleIds.length}`
      );
    }
  }

  console.info(
    `Pinecone 정리 완료: API ids ${current.ids.size}건, ` +
      `Pinecone ids ${pineconeIds.length}건, stale ${staleIds.length}건, ` +
      `deleted ${deleted}건, dryRun ${dryRun}`
  );

  return {
    ok: true,
    dryRun,
    apiRows: current.apiRows,
    apiIds: current.ids.size,
    pineconeIds: pineconeIds.length,
    staleIds: staleIds.length,
    deleted,
  };
}
