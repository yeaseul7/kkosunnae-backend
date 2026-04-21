import * as logger from "firebase-functions/logger";
import {defineSecret} from "firebase-functions/params";
import {onRequest} from "firebase-functions/v2/https";
import {
  fetchAbandonmentApiPage,
  mergeShelterAnimalsIntoFirestore,
  parseShelterItemList,
  SHELTER_API_PAGE_SIZE,
} from "./shelterAnimalFirestoreWrite.js";

const ANIMALS_OPENAPI_SECRET = defineSecret("ANIMALS_OPENAPI_KEY");
/** HTTP 호출 시 헤더 x-backfill-token 값과 일치해야 실행 (남용 방지) */
const BACKFILL_HTTP_TOKEN = defineSecret("SHELTER_BACKFILL_HTTP_TOKEN");

/** 500건 × 20페이지 ≈ 1만 건 */
const BACKFILL_PAGE_COUNT = 20;
/** 페이지 간 API 부담 완화(ms) */
const BETWEEN_PAGES_MS = 400;

/**
 * @param {number} ms 대기 시간(ms)
 * @return {Promise<void>} ms 후 resolve
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * HTTP 수동 호출: 유기동물 공공 API 20페이지(페이지당 500건)를 읽어
 * Pinecone 없이 Firestore shelterAnimals에만 merge 저장
 */
export const backfillShelterAnimalsToFirestore = onRequest(
  {
    secrets: [ANIMALS_OPENAPI_SECRET, BACKFILL_HTTP_TOKEN],
    memory: "1GiB",
    timeoutSeconds: 540,
    cors: false,
  },
  async (req, res) => {
    const expected = BACKFILL_HTTP_TOKEN.value();
    const sent = req.get("x-backfill-token") ?? "";
    if (!expected || sent !== expected) {
      res.status(403).send("Forbidden");
      return;
    }

    const serviceKey = ANIMALS_OPENAPI_SECRET.value();
    if (!serviceKey) {
      res.status(500).send("ANIMALS_OPENAPI_KEY missing");
      return;
    }

    if (req.method !== "POST") {
      res.status(405).send("Use POST");
      return;
    }

    try {
      let totalWritten = 0;
      let pagesFetched = 0;
      for (let pageNo = 1; pageNo <= BACKFILL_PAGE_COUNT; pageNo++) {
        const data = await fetchAbandonmentApiPage(
          serviceKey,
          pageNo,
          SHELTER_API_PAGE_SIZE
        );
        const {itemList, totalCount} = parseShelterItemList(data);
        const written = await mergeShelterAnimalsIntoFirestore(itemList);
        totalWritten += written;
        pagesFetched = pageNo;
        logger.info(
          `백필 page ${pageNo}/${BACKFILL_PAGE_COUNT}: ` +
            `응답 ${itemList.length}건, Firestore ${written}건 ` +
            `(API totalCount ${totalCount})`
        );
        if (itemList.length === 0) {
          logger.info("빈 페이지로 중단");
          break;
        }
        if (pageNo < BACKFILL_PAGE_COUNT) {
          await delay(BETWEEN_PAGES_MS);
        }
      }
      res.status(200).json({
        ok: true,
        pagesFetched,
        firestoreDocWrites: totalWritten,
        rowsPerPage: SHELTER_API_PAGE_SIZE,
      });
    } catch (error) {
      logger.error("Firestore 백필 실패:", error);
      res.status(500).json({
        ok: false,
        error: String(error),
      });
    }
  }
);
