import * as logger from "firebase-functions/logger";
import {defineSecret} from "firebase-functions/params";
import {onSchedule} from "firebase-functions/scheduler";
import {Pinecone} from "@pinecone-database/pinecone";
import {pipeline} from "@huggingface/transformers";
import {AbandonmentApiResponse, ShelterAnimalItem} from "./types.js";

const API_BASE_URL =
  "https://apis.data.go.kr/1543061/abandonmentPublicService_v2";
const ANIMALS_OPENAPI_SECRET = defineSecret("ANIMALS_OPENAPI_KEY");
const PINECONE_API_SECRET = defineSecret("PINECONE_API_KEY");
/** Pinecone 인덱스 dimension: 384 (DINOv2-small) */
const PINECONE_INDEX_NAME = "embeded-animal";
const TARGET_SAVE_COUNT = 100;
const ROWS_PER_PAGE = 100;
const FETCH_IDS_BATCH_SIZE = 100;
const UPSERT_BATCH_SIZE = 100;
const IMAGE_MODEL_ID = "Xenova/dinov2-small";

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
  opts?: { pool?: boolean }
) => Promise<{ data: Float32Array; dims?: number[] }>;

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
 * 공공데이터 API에서 한 페이지 조회
 * @param {string} serviceKey 공공데이터 API 인증키
 * @param {number} pageNo 페이지 번호
 * @param {number} numOfRows 페이지당 행 수
 */
async function fetchAbandonmentApiPage(
  serviceKey: string,
  pageNo: number,
  numOfRows: number = ROWS_PER_PAGE
): Promise<AbandonmentApiResponse> {
  const params = new URLSearchParams({
    serviceKey,
    pageNo: String(pageNo),
    numOfRows: String(numOfRows),
    _type: "json",
  });

  const url = `${API_BASE_URL}/abandonmentPublic_v2?${params.toString()}`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`API 오류: ${response.status} ${await response.text()}`);
  }

  return response.json() as Promise<AbandonmentApiResponse>;
}

/**
 * 메인 동기화: 공공데이터 조회 → id 존재 시 스킵 → popfile 벡터화 → Pinecone 저장
 * (최대 TARGET_SAVE_COUNT개)
 * @param {string} serviceKey 공공데이터 API 인증키
 * @param {string} pineconeApiKey Pinecone API 키
 */
async function runSync(
  serviceKey: string,
  pineconeApiKey: string
): Promise<void> {
  const pc = new Pinecone({apiKey: pineconeApiKey});
  const index = pc.index(PINECONE_INDEX_NAME);

  logger.info("이미지 임베딩 모델 로딩 중...");
  const pipe = await pipeline("image-feature-extraction", IMAGE_MODEL_ID);
  const extractor = pipe as unknown as FeatureExtractor;

  let pageNo = 1;
  let totalCount = 0;
  let processed = 0;
  let skipped = 0;
  let embedded = 0;

  do {
    const data = await fetchAbandonmentApiPage(serviceKey, pageNo);
    const body = data?.response?.body;
    const items = body?.items?.item;

    if (!body?.totalCount) {
      totalCount = 0;
      break;
    }
    totalCount = body.totalCount;

    const itemList: ShelterAnimalItem[] = Array.isArray(items) ?
      items :
      items ?
        [items] :
        [];

    type Candidate =
      { docId: string; item: ShelterAnimalItem; imageUrl: string };
    const candidates: Candidate[] = [];
    for (const item of itemList) {
      const desertionNo = item.desertionNo;
      const careRegNo = item.careRegNo ?? "";
      if (!desertionNo || !careRegNo) continue;
      const docId = `${desertionNo}-${careRegNo}`;
      const imageUrl = getPopfileUrl(item);
      if (!imageUrl) continue;
      candidates.push({docId, item, imageUrl});
    }

    if (candidates.length === 0) {
      pageNo++;
      continue;
    }

    const existingIds = new Set<string>();
    for (let i = 0; i < candidates.length; i += FETCH_IDS_BATCH_SIZE) {
      const batch = candidates
        .slice(i, i + FETCH_IDS_BATCH_SIZE)
        .map((c) => c.docId);
      const result = await index.fetch({ids: batch});
      if (result?.records) {
        for (const id of Object.keys(result.records)) {
          existingIds.add(id);
        }
      }
    }

    const recordsToUpsert: Array<{
      id: string;
      values: number[];
      metadata: { desertionNo: string; careRegNo: string; imageUrl: string };
    }> = [];

    for (const {docId, item, imageUrl} of candidates) {
      if (embedded >= TARGET_SAVE_COUNT) break;

      processed++;
      if (existingIds.has(docId)) {
        skipped++;
        continue;
      }

      const embedding = await getImageEmbedding(extractor, imageUrl);
      if (!embedding) continue;

      recordsToUpsert.push({
        id: docId,
        values: embedding,
        metadata: {
          desertionNo: String(item.desertionNo ?? ""),
          careRegNo: String(item.careRegNo ?? ""),
          imageUrl,
        },
      });

      if (recordsToUpsert.length >= UPSERT_BATCH_SIZE) {
        await index.upsert({records: recordsToUpsert});
        embedded += recordsToUpsert.length;
        logger.info(
          `배치 저장: ${recordsToUpsert.length}건 ` +
          `(${embedded}/${TARGET_SAVE_COUNT})`
        );
        recordsToUpsert.length = 0;
      }
    }

    if (recordsToUpsert.length > 0) {
      await index.upsert({records: recordsToUpsert});
      embedded += recordsToUpsert.length;
      logger.info(
        `배치 저장: ${recordsToUpsert.length}건 (${embedded}/${TARGET_SAVE_COUNT})`
      );
    }

    if (embedded >= TARGET_SAVE_COUNT) {
      break;
    }
    pageNo++;
  } while (pageNo <= Math.ceil(totalCount / ROWS_PER_PAGE));

  logger.info(
    `동기화 완료: 전체 ${totalCount}건, 처리 ${processed}건, ` +
    `기존 스킵 ${skipped}건, 신규 저장 ${embedded}건 (목표 ${TARGET_SAVE_COUNT}건)`
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
