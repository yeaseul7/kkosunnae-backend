import {getApps, initializeApp} from "firebase-admin/app";
import {
  FieldPath,
  FieldValue,
  QueryDocumentSnapshot,
  getFirestore,
} from "firebase-admin/firestore";
import * as logger from "firebase-functions/logger";
import {defineSecret} from "firebase-functions/params";
import {onSchedule} from "firebase-functions/scheduler";
import {
  fetchAbandonmentApiPage,
  parseShelterItemList,
  SHELTER_ANIMALS_COLLECTION,
  SHELTER_API_PAGE_SIZE,
} from "./shelterAnimalFirestoreWrite.js";

const ANIMALS_OPENAPI_SECRET = defineSecret("ANIMALS_OPENAPI_KEY");
const FIRESTORE_BATCH_SIZE = 400;
const FIRESTORE_READ_PAGE_SIZE = 1000;

if (!getApps().length) {
  initializeApp();
}
const firestore = getFirestore();

/**
 * @param {string | undefined} value 원본 문자열
 * @return {string} trim된 문자열
 */
function normalize(value: string | undefined): string {
  return String(value ?? "").trim();
}

/**
 * API 전체 페이지를 순회해 현재 유기동물 상태 맵을 생성
 * @param {string} serviceKey 공공데이터 API 키
 * @return {Promise<object>} docId -> 상태(processState, neuterYn) 맵
 */
async function fetchCurrentAnimalsMap(
  serviceKey: string
): Promise<Map<string, {processState: string; neuterYn: string}>> {
  const result = new Map<string, {processState: string; neuterYn: string}>();
  let pageNo = 1;
  let totalPages = 1;

  while (pageNo <= totalPages) {
    const data = await fetchAbandonmentApiPage(
      serviceKey,
      pageNo,
      SHELTER_API_PAGE_SIZE
    );
    const {itemList, totalCount} = parseShelterItemList(data);
    totalPages = Math.max(1, Math.ceil(totalCount / SHELTER_API_PAGE_SIZE));

    for (const item of itemList) {
      const desertionNo = normalize(item.desertionNo);
      const careRegNo = normalize(item.careRegNo);
      if (!desertionNo || !careRegNo) continue;
      const docId = `${desertionNo}-${careRegNo}`;
      result.set(docId, {
        processState: normalize(item.processState),
        neuterYn: normalize(item.neuterYn),
      });
    }

    logger.info(
      `상태 동기화 API 조회: page ${pageNo}/${totalPages}, 누적 ${result.size}건`
    );
    pageNo++;
  }

  return result;
}

/**
 * shelterAnimals 전체와 API 현재값을 비교해
 * 1) processState/neuterYn 변경 시 해당 필드만 업데이트
 * 2) API에서 사라진 문서는 삭제
 * @param {object} currentMap API 현재 상태 맵
 * @return {Promise<object>} updated/deleted/unchanged 통계
 */
async function reconcileShelterAnimals(
  currentMap: Map<string, {processState: string; neuterYn: string}>
): Promise<{updated: number; deleted: number; unchanged: number}> {
  let updated = 0;
  let deleted = 0;
  let unchanged = 0;
  let scanned = 0;
  let lastDoc: QueryDocumentSnapshot | undefined;

  let hasMore = true;
  while (hasMore) {
    let query = firestore
      .collection(SHELTER_ANIMALS_COLLECTION)
      .orderBy(FieldPath.documentId())
      .limit(FIRESTORE_READ_PAGE_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }
    const snap = await query.get();
    if (snap.empty) {
      hasMore = false;
      continue;
    }
    const docs = snap.docs;
    lastDoc = docs[docs.length - 1];

    for (let i = 0; i < docs.length; i += FIRESTORE_BATCH_SIZE) {
      const slice = docs.slice(i, i + FIRESTORE_BATCH_SIZE);
      const batch = firestore.batch();
      for (const doc of slice) {
        const apiState = currentMap.get(doc.id);
        if (!apiState) {
          batch.delete(doc.ref);
          deleted++;
          continue;
        }

        const data = doc.data() as Record<string, unknown>;
        const beforeState = normalize(String(data.processState ?? ""));
        const beforeNeuterYn = normalize(
          String((data.neuterYn ?? data.neuter_yn ?? "") as string)
        );
        const afterState = apiState.processState;
        const afterNeuterYn = apiState.neuterYn;

        if (beforeState === afterState && beforeNeuterYn === afterNeuterYn) {
          unchanged++;
          continue;
        }

        batch.set(doc.ref, {
          processState: afterState,
          neuterYn: afterNeuterYn,
          // 기존 스키마에 snake_case가 남아있다면 같이 맞춤
          neuter_yn: afterNeuterYn,
          statusUpdatedAt: FieldValue.serverTimestamp(),
        }, {merge: true});
        updated++;
      }
      await batch.commit();
    }

    scanned += docs.length;
    logger.info(
      `상태 동기화 스캔 진행: ${scanned}건, 업데이트 ${updated}, 삭제 ${deleted}`
    );
  }

  return {updated, deleted, unchanged};
}

/**
 * 매일 shelterAnimals 상태/중성화 여부 증분 동기화 + 사라진 문서 삭제
 */
export const syncShelterAnimalsStatus = onSchedule(
  {
    schedule: "0 18 * * *", // UTC 18:00 = KST 03:00
    timeZone: "Asia/Seoul",
    secrets: [ANIMALS_OPENAPI_SECRET],
    memory: "1GiB",
    timeoutSeconds: 540,
  },
  async () => {
    const serviceKey = ANIMALS_OPENAPI_SECRET.value();
    if (!serviceKey) {
      logger.error("ANIMALS_OPENAPI_KEY 시크릿이 설정되지 않았습니다.");
      return;
    }

    const currentMap = await fetchCurrentAnimalsMap(serviceKey);
    const {updated, deleted, unchanged} = await reconcileShelterAnimals(
      currentMap
    );
    logger.info(
      `shelterAnimals 상태 동기화 완료: API ${currentMap.size}건, ` +
        `업데이트 ${updated}건, 삭제 ${deleted}건, 변경없음 ${unchanged}건`
    );
  }
);
