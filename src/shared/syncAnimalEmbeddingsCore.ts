import {Pinecone} from "@pinecone-database/pinecone";
import {
  fetchAbandonmentApiPage,
  normalizeHappenYyyyMmDd,
  parseShelterItemList,
  SHELTER_API_PAGE_SIZE,
  todayYyyyMmDdSeoul,
} from "./shelterAnimalFirestoreWrite.js";
import {
  PINECONE_INDEX_NAME,
  embedImage,
} from "./imageEmbedding.js";
import {ShelterAnimalItem} from "./types.js";

const UPSERT_BATCH_SIZE = 100;
/** 청크 단위: 한 번에 벡터화할 개수 (공공 API·메모리 부담 완화) */
const CHUNK_SIZE = 30;
/** 청크 간 휴식 시간(ms). 공공기관 서버·메모리 안정용 */
const DELAY_BETWEEN_CHUNKS_MS = 1500;

/**
 * @param {number} ms 대기 시간(ms)
 * @return {Promise<void>} ms 후 resolve
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * 공공데이터 API에서 popfile(첫 번째 이미지) URL만 추출
 * popfile1(popfile)만 조회, popfile2/popfile3 등 제외
 * @param {ShelterAnimalItem} item 유기동물 API 응답 항목
 * @return {string | null} popfile URL 또는 없으면 null
 */
function getPopfileUrl(item: ShelterAnimalItem): string | null {
  const val = item.popfile ?? item.popfile1;
  if (!val || typeof val !== "string") return null;
  const trimmed = val.trim();
  return trimmed.startsWith("http") ? trimmed : null;
}

/**
 * 메인 동기화: 공공데이터 1페이지(500건) 조회
 * → 오늘 유기분 중 popfile 있는 항목만 이미지 벡터화 후 Pinecone upsert
 * @param {string} serviceKey 공공데이터 API 인증키
 * @param {string} pineconeApiKey Pinecone API 키
 */
export async function runSync(
  serviceKey: string,
  pineconeApiKey: string
): Promise<void> {
  const pc = new Pinecone({apiKey: pineconeApiKey});
  const index = pc.index(PINECONE_INDEX_NAME);

  const todayYmd = todayYyyyMmDdSeoul();
  const data = await fetchAbandonmentApiPage(
    serviceKey,
    1,
    SHELTER_API_PAGE_SIZE
  );
  const {itemList, totalCount} = parseShelterItemList(data);

  type Candidate =
    { docId: string; item: ShelterAnimalItem; imageUrl: string };
  const candidates: Candidate[] = [];
  for (const item of itemList) {
    if (normalizeHappenYyyyMmDd(item.happenDt) !== todayYmd) continue;
    const desertionNo = item.desertionNo;
    const careRegNo = item.careRegNo ?? "";
    if (!desertionNo || !careRegNo) continue;
    const docId = `${desertionNo}-${careRegNo}`;
    const imageUrl = getPopfileUrl(item);
    if (!imageUrl) continue;
    candidates.push({docId, item, imageUrl});
  }

  const toEmbed = candidates;
  let embedded = 0;
  let embedFailed = 0;

  const recordsToUpsert: Array<{
    id: string;
    values: number[];
    metadata: {
      desertionNo: string;
      careRegNo: string;
      imageUrl: string;
      upKindCd: string;
      orgNm: string;
    };
  }> = [];

  for (let i = 0; i < toEmbed.length; i += CHUNK_SIZE) {
    const chunk = toEmbed.slice(i, i + CHUNK_SIZE);
    const chunkResults = await Promise.all(
      chunk.map(async ({docId, item, imageUrl}) => {
        const embedding = await embedImage(imageUrl);
        if (!embedding) return null;
        return {
          id: docId,
          values: embedding,
          metadata: {
            desertionNo: String(item.desertionNo ?? ""),
            careRegNo: String(item.careRegNo ?? ""),
            imageUrl,
            upKindCd: String(item.upKindCd ?? ""),
            orgNm: String(item.orgNm ?? ""),
          },
        };
      })
    );

    for (const rec of chunkResults) {
      if (!rec) {
        embedFailed++;
        continue;
      }
      recordsToUpsert.push(rec);
      embedded++;
    }

    if (recordsToUpsert.length >= UPSERT_BATCH_SIZE) {
      await index.upsert({records: recordsToUpsert});
      console.info(
        `배치 저장: ${recordsToUpsert.length}건 ` +
          `(누적 임베딩 성공 ${embedded}건)`
      );
      recordsToUpsert.length = 0;
    }

    if (i + CHUNK_SIZE < toEmbed.length) {
      await delay(DELAY_BETWEEN_CHUNKS_MS);
    }
  }

  if (recordsToUpsert.length > 0) {
    await index.upsert({records: recordsToUpsert});
    console.info(`배치 저장: ${recordsToUpsert.length}건 (누적 임베딩 성공 ${embedded}건)`);
  }

  console.info(
    `동기화 완료: API totalCount ${totalCount}건, 1페이지 응답 ${itemList.length}건, ` +
    `유기일 ${todayYmd}·이미지 있음 후보 ${candidates.length}건, ` +
    `Pinecone 임베딩 성공 ${embedded}건, 이미지 실패 ${embedFailed}건`
  );
}
