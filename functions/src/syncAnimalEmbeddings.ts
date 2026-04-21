import * as logger from "firebase-functions/logger";
import {defineSecret} from "firebase-functions/params";
import {onSchedule} from "firebase-functions/scheduler";
import {Pinecone} from "@pinecone-database/pinecone";
import {pipeline} from "@huggingface/transformers";
import {
  fetchAbandonmentApiPage,
  mergeShelterAnimalsIntoFirestore,
  normalizeHappenYyyyMmDd,
  parseShelterItemList,
  SHELTER_ANIMALS_COLLECTION,
  SHELTER_API_PAGE_SIZE,
  todayYyyyMmDdSeoul,
} from "./shelterAnimalFirestoreWrite.js";
import {ShelterAnimalItem} from "./types.js";

const ANIMALS_OPENAPI_SECRET = defineSecret("ANIMALS_OPENAPI_KEY");
const PINECONE_API_SECRET = defineSecret("PINECONE_API_KEY");
/** Pinecone 인덱스 dimension: 384 (DINOv2-small) */
const PINECONE_INDEX_NAME = "embeded-animal";
const UPSERT_BATCH_SIZE = 100;
/** 청크 단위: 한 번에 벡터화할 개수 (공공 API·메모리 부담 완화) */
const CHUNK_SIZE = 30;
/** 청크 간 휴식 시간(ms). 공공기관 서버·메모리 안정용 */
const DELAY_BETWEEN_CHUNKS_MS = 1500;
const IMAGE_MODEL_ID = "Xenova/dinov2-small";

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

/** image-feature-extraction 파이프라인 타입 (pooler 없음 → dims로 CLS 추출) */
type FeatureExtractor = (
  url: string,
  opts?: {pool?: boolean}
) => Promise<{data: Float32Array; dims?: number[]}>;

/**
 * DINOv2는 pooler 없음 → pool: false 후 CLS(첫 토큰) 추출, L2 정규화로 코사인 유사도 최적화
 * 결과: 384차원 벡터
 * @param {FeatureExtractor} extractor image-feature-extraction pipeline 인스턴스
 * @param {string} imageUrl 프로필 이미지 URL
 */
async function getImageEmbedding(
  extractor: FeatureExtractor,
  imageUrl: string
): Promise<number[] | null> {
  try {
    const result = await extractor(imageUrl, {pool: false});
    if (!result?.data) return null;

    const data = result.data as Float32Array;
    const dims = result.dims ?? [];
    const hiddenDim = dims.length >= 3 ? dims[2] : data.length;
    const clsVector = data.subarray(0, hiddenDim);

    const arr = Array.from(clsVector);
    const norm = Math.sqrt(arr.reduce((s, x) => s + x * x, 0)) || 1;
    return arr.map((x) => x / norm);
  } catch (error) {
    logger.warn(`이미지 벡터화 실패 (${imageUrl}):`, error);
    return null;
  }
}

/**
 * 메인 동기화: 공공데이터 1페이지(500건) 조회 → happenDt 오늘분 Firestore merge 저장
 * → popfile 있는 항목만 이미지 벡터화 후 Pinecone upsert
 * @param {string} serviceKey 공공데이터 API 인증키
 * @param {string} pineconeApiKey Pinecone API 키
 */
async function runSync(
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

  const firestoreWritten = await mergeShelterAnimalsIntoFirestore(
    itemList,
    todayYmd
  );
  logger.info(
    `Firestore 저장 완료: 유기일 ${todayYmd} 대상 ${firestoreWritten}건 ` +
      `(컬렉션: ${SHELTER_ANIMALS_COLLECTION})`
  );

  logger.info(
    "이미지 임베딩 모델 로딩 중… " +
      `(대상 유기일 happenDt = ${todayYmd}, 1페이지 ${SHELTER_API_PAGE_SIZE}건)`
  );
  const pipe = await pipeline("image-feature-extraction", IMAGE_MODEL_ID);
  const extractor = pipe as unknown as FeatureExtractor;

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
        const embedding = await getImageEmbedding(extractor, imageUrl);
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
      logger.info(`배치 저장: ${recordsToUpsert.length}건 (누적 임베딩 성공 ${embedded}건)`);
      recordsToUpsert.length = 0;
    }

    if (i + CHUNK_SIZE < toEmbed.length) {
      await delay(DELAY_BETWEEN_CHUNKS_MS);
    }
  }

  if (recordsToUpsert.length > 0) {
    await index.upsert({records: recordsToUpsert});
    logger.info(`배치 저장: ${recordsToUpsert.length}건 (누적 임베딩 성공 ${embedded}건)`);
  }

  logger.info(
    `동기화 완료: API totalCount ${totalCount}건, 1페이지 응답 ${itemList.length}건, ` +
    `Firestore(오늘) ${firestoreWritten}건, ` +
    `유기일 ${todayYmd}·이미지 있음 후보 ${candidates.length}건, ` +
    `Pinecone 임베딩 성공 ${embedded}건, 이미지 실패 ${embedFailed}건`
  );
}

/**
 * GCP Cloud Scheduler에 의해 실행되는 스케줄 함수
 * 기본: 매일 새벽 2시(KST) 실행 (cron: 0 17 * * * = UTC 17:00 = KST 02:00)
 */
export const syncAnimalEmbeddings = onSchedule(
  {
    schedule: "0 17 * * *", // UTC 17:00 = KST 02:00
    timeZone: "Asia/Seoul",
    secrets: [ANIMALS_OPENAPI_SECRET, PINECONE_API_SECRET],
    memory: "2GiB",
    timeoutSeconds: 540,
  },
  async () => {
    const serviceKey = ANIMALS_OPENAPI_SECRET.value();
    const pineconeApiKey = PINECONE_API_SECRET.value();

    if (!serviceKey) {
      logger.error("ANIMALS_OPENAPI_KEY 시크릿이 설정되지 않았습니다.");
      return;
    }
    if (!pineconeApiKey) {
      logger.error("PINECONE_API_KEY 시크릿이 설정되지 않았습니다.");
      return;
    }

    try {
      await runSync(serviceKey, pineconeApiKey);
    } catch (error) {
      logger.error("동물 프로필 벡터화 동기화 실패 ㅠㅠ:", error);
      throw error;
    }
  }
);
