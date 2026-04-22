import {getApps, initializeApp} from "firebase-admin/app";
import {FieldValue, getFirestore} from "firebase-admin/firestore";

const SHELTERS_API_BASE_URL =
  "https://apis.data.go.kr/1543061/animalShelterSrvc_v2";
const SHELTER_INFO_COLLECTION = "shelter-info";
const SHELTER_INFO_NUM_OF_ROWS = 500;
const SHELTER_INFO_MAX_PAGES = 20;
const FIRESTORE_BATCH_SIZE = 400;
const BETWEEN_PAGES_MS = 400;

if (!getApps().length) {
  initializeApp();
}
const firestore = getFirestore();

type ShelterInfoItem = Record<string, string | number | boolean | null>;
type ShelterInfoApiResponse = {
  response?: {
    body?: {
      items?: {
        item?: ShelterInfoItem | ShelterInfoItem[] | null;
      };
      totalCount?: number;
    };
  };
};

export interface BackfillShelterInfoResult {
  ok: true;
  pagesFetched: number;
  firestoreDocWrites: number;
  rowsPerPage: number;
  firestoreCollection: string;
}

/**
 * @param {number} ms 대기 시간(ms)
 * @return {Promise<void>} ms 후 resolve
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * 응답 item을 배열로 정규화
 * @param {ShelterInfoApiResponse} data API 응답 JSON
 * @return {ShelterInfoItem[]} item 배열
 */
function parseItems(data: ShelterInfoApiResponse): ShelterInfoItem[] {
  const rawItems = data?.response?.body?.items?.item;
  return Array.isArray(rawItems) ? rawItems : rawItems ? [rawItems] : [];
}

/**
 * 공공데이터 보호소 API를 페이지 단위로 조회
 * @param {string} serviceKey 공공데이터 API 키
 * @param {number} pageNo 페이지 번호
 * @return {Promise<ShelterInfoApiResponse>} API 응답 JSON
 */
async function fetchShelterInfoPage(
  serviceKey: string,
  pageNo: number
): Promise<ShelterInfoApiResponse> {
  const params = new URLSearchParams({
    serviceKey,
    pageNo: String(pageNo),
    numOfRows: String(SHELTER_INFO_NUM_OF_ROWS),
    _type: "json",
  });
  const url = `${SHELTERS_API_BASE_URL}/shelterInfo_v2?${params.toString()}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(
      `보호소 API 오류: ${response.status} ${await response.text()}`
    );
  }
  return response.json() as Promise<ShelterInfoApiResponse>;
}

/**
 * API item을 Firestore 저장용 plain object로 정규화
 * @param {ShelterInfoItem} item 보호소 항목
 * @return {Record<string, string | number | boolean>} 정규화된 필드
 */
function normalizeShelterInfoItem(
  item: ShelterInfoItem
): Record<string, string | number | boolean> {
  const out: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(item)) {
    if (value === undefined || value === null) continue;
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean"
    ) {
      out[key] = value;
    } else {
      out[key] = String(value);
    }
  }
  return out;
}

/**
 * 보호소 목록을 shelter-info 컬렉션에 merge 저장
 * @param {ShelterInfoItem[]} items 보호소 항목 목록
 * @return {Promise<number>} 저장한 문서 수
 */
async function mergeShelterInfoIntoFirestore(
  items: ShelterInfoItem[]
): Promise<number> {
  const candidates: Array<{
    docId: string;
    fields: Record<string, unknown>;
  }> = [];
  for (const item of items) {
    const careRegNo = String(item.careRegNo ?? "").trim();
    if (!careRegNo) continue;
    candidates.push({
      docId: careRegNo,
      fields: {
        ...normalizeShelterInfoItem(item),
        updatedAt: FieldValue.serverTimestamp(),
      },
    });
  }

  let written = 0;
  let skippedExisting = 0;
  for (let i = 0; i < candidates.length; i += FIRESTORE_BATCH_SIZE) {
    const slice = candidates.slice(i, i + FIRESTORE_BATCH_SIZE);
    const refs = slice.map(({docId}) =>
      firestore.collection(SHELTER_INFO_COLLECTION).doc(docId)
    );
    const snaps = await firestore.getAll(...refs);
    const newRows = slice.filter((_, idx) => !snaps[idx].exists);
    skippedExisting += slice.length - newRows.length;
    if (newRows.length === 0) continue;

    const batch = firestore.batch();
    for (const {docId, fields} of newRows) {
      const ref = firestore.collection(SHELTER_INFO_COLLECTION).doc(docId);
      batch.set(ref, fields);
    }
    await batch.commit();
    written += newRows.length;
  }
  if (skippedExisting > 0) {
    console.info(`기존 보호소 스킵: ${skippedExisting}건`);
  }
  return written;
}

/**
 * shelterInfo_v2 전체를 조회 후 Firestore shelter-info 컬렉션에 증분 저장
 * @param {string} serviceKey 공공데이터 API 키
 * @return {Promise<BackfillShelterInfoResult>} 백필 결과
 */
export async function backfillShelterInfoToFirestore(
  serviceKey: string
): Promise<BackfillShelterInfoResult> {
  let pagesFetched = 0;
  let totalWritten = 0;

  for (let pageNo = 1; pageNo <= SHELTER_INFO_MAX_PAGES; pageNo++) {
    const data = await fetchShelterInfoPage(serviceKey, pageNo);
    const items = parseItems(data);
    const totalCount = data?.response?.body?.totalCount ?? 0;
    const written = await mergeShelterInfoIntoFirestore(items);
    pagesFetched = pageNo;
    totalWritten += written;
    console.info(
      `보호소 백필 page ${pageNo}/${SHELTER_INFO_MAX_PAGES}: ` +
        `응답 ${items.length}건, Firestore ${written}건 ` +
        `(API totalCount ${totalCount})`
    );
    if (items.length === 0) {
      console.info("보호소 백필 빈 페이지로 중단");
      break;
    }
    if (pageNo < SHELTER_INFO_MAX_PAGES) {
      await delay(BETWEEN_PAGES_MS);
    }
  }

  console.info(
    `보호소 백필 완료: pagesFetched ${pagesFetched}, ` +
      `rowsPerPage ${SHELTER_INFO_NUM_OF_ROWS}, ` +
      `firestoreCollection ${SHELTER_INFO_COLLECTION}, ` +
      `firestoreDocWrites ${totalWritten}`
  );

  return {
    ok: true,
    pagesFetched,
    rowsPerPage: SHELTER_INFO_NUM_OF_ROWS,
    firestoreCollection: SHELTER_INFO_COLLECTION,
    firestoreDocWrites: totalWritten,
  };
}
