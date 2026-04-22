import {getApps, initializeApp} from "firebase-admin/app";
import {
  FieldPath,
  FieldValue,
  QueryDocumentSnapshot,
  getFirestore,
} from "firebase-admin/firestore";
import {
  fetchAbandonmentApiPage,
  mergeShelterAnimalsIntoFirestore,
  parseShelterItemList,
  SHELTER_ANIMALS_COLLECTION,
  SHELTER_API_PAGE_SIZE,
} from "../shared/shelterAnimalFirestoreWrite.js";
import {ShelterAnimalItem} from "../shared/types.js";

const FIRESTORE_BATCH_SIZE = 400;
const FIRESTORE_READ_PAGE_SIZE = 1000;

if (!getApps().length) {
  initializeApp();
}
const firestore = getFirestore();

export interface SyncShelterAnimalsStatusResult {
  ok: true;
  apiRows: number;
  upserted: number;
  updated: number;
  deleted: number;
  unchanged: number;
}

interface CurrentAnimalsSnapshot {
  stateMap: Map<string, {processState: string; neuterYn: string}>;
  itemList: ShelterAnimalItem[];
}

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
): Promise<CurrentAnimalsSnapshot> {
  const stateMap = new Map<string, {processState: string; neuterYn: string}>();
  const itemList: ShelterAnimalItem[] = [];
  let pageNo = 1;
  let totalPages = 1;

  while (pageNo <= totalPages) {
    const data = await fetchAbandonmentApiPage(
      serviceKey,
      pageNo,
      SHELTER_API_PAGE_SIZE
    );
    const {itemList: pageItems, totalCount} = parseShelterItemList(data);
    totalPages = Math.max(1, Math.ceil(totalCount / SHELTER_API_PAGE_SIZE));
    itemList.push(...pageItems);

    for (const item of pageItems) {
      const desertionNo = normalize(item.desertionNo);
      const careRegNo = normalize(item.careRegNo);
      if (!desertionNo || !careRegNo) continue;
      const docId = `${desertionNo}-${careRegNo}`;
      stateMap.set(docId, {
        processState: normalize(item.processState),
        neuterYn: normalize(item.neuterYn),
      });
    }

    console.info(
      `상태 동기화 API 조회: page ${pageNo}/${totalPages}, 누적 ${stateMap.size}건`
    );
    pageNo++;
  }

  return {stateMap, itemList};
}

/**
 * shelterAnimals 전체와 API 현재값을 비교해 상태 변경/삭제를 반영
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
          neuter_yn: afterNeuterYn,
          statusUpdatedAt: FieldValue.serverTimestamp(),
        }, {merge: true});
        updated++;
      }
      await batch.commit();
    }

    scanned += docs.length;
    console.info(
      `상태 동기화 스캔 진행: ${scanned}건, 업데이트 ${updated}, 삭제 ${deleted}`
    );
  }

  return {updated, deleted, unchanged};
}

/**
 * shelterAnimals 신규 추가 + 상태/중성화 여부 증분 동기화 + 사라진 문서 삭제
 * @param {string} serviceKey 공공데이터 API 키
 * @return {Promise<SyncShelterAnimalsStatusResult>} 동기화 결과
 */
export async function syncShelterAnimalsStatus(
  serviceKey: string
): Promise<SyncShelterAnimalsStatusResult> {
  const current = await fetchCurrentAnimalsMap(serviceKey);
  const upserted = await mergeShelterAnimalsIntoFirestore(current.itemList);
  const {updated, deleted, unchanged} = await reconcileShelterAnimals(
    current.stateMap
  );
  console.info(
    `shelterAnimals 동기화 완료: API ${current.stateMap.size}건, ` +
      `추가/병합 ${upserted}건, ` +
      `업데이트 ${updated}건, 삭제 ${deleted}건, 변경없음 ${unchanged}건`
  );

  return {
    ok: true,
    apiRows: current.stateMap.size,
    upserted,
    updated,
    deleted,
    unchanged,
  };
}
