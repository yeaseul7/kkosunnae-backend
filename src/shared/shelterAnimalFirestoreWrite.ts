import {AbandonmentApiResponse, ShelterAnimalItem} from "./types.js";

export const ABANDONMENT_API_BASE_URL =
  "https://apis.data.go.kr/1543061/abandonmentPublicService_v2";

/** 공공 API 페이지당 행 수(상한 500) */
export const SHELTER_API_PAGE_SIZE = 500;

/**
 * Asia/Seoul 기준 오늘 날짜를 YYYYMMDD로 반환
 * @return {string} 예: 20260421
 */
export function todayYyyyMmDdSeoul(): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const y = parts.find((p) => p.type === "year")?.value ?? "";
  const m = parts.find((p) => p.type === "month")?.value ?? "";
  const d = parts.find((p) => p.type === "day")?.value ?? "";
  return `${y}${m.padStart(2, "0")}${d.padStart(2, "0")}`;
}

/**
 * API happenDt를 YYYYMMDD 8자리로 정규화 (앞 8자만 사용)
 * @param {string | undefined} happenDt 유기일자
 * @return {string} YYYYMMDD 또는 8자 미만이면 trim된 문자열
 */
export function normalizeHappenYyyyMmDd(
  happenDt: string | undefined
): string {
  const s = String(happenDt ?? "").trim();
  return s.length >= 8 ? s.slice(0, 8) : s;
}

/**
 * abandonmentPublic_v2 JSON에서 목록·totalCount 추출
 * @param {AbandonmentApiResponse} data API JSON
 * @return {object} itemList, totalCount 필드를 가진 파싱 결과
 */
export function parseShelterItemList(
  data: AbandonmentApiResponse
): {itemList: ShelterAnimalItem[]; totalCount: number} {
  const body = data?.response?.body;
  const items = body?.items?.item;
  const totalCount = body?.totalCount ?? 0;
  const itemList: ShelterAnimalItem[] = Array.isArray(items) ?
    items :
    items ?
      [items] :
      [];
  return {itemList, totalCount};
}

/**
 * 공공데이터 API에서 한 페이지 조회
 * @param {string} serviceKey 공공데이터 API 인증키
 * @param {number} pageNo 페이지 번호
 * @param {number} numOfRows 페이지당 행 수
 */
export async function fetchAbandonmentApiPage(
  serviceKey: string,
  pageNo: number,
  numOfRows: number = SHELTER_API_PAGE_SIZE
): Promise<AbandonmentApiResponse> {
  const params = new URLSearchParams({
    serviceKey,
    pageNo: String(pageNo),
    numOfRows: String(numOfRows),
    _type: "json",
  });

  const url =
    `${ABANDONMENT_API_BASE_URL}/abandonmentPublic_v2?${params.toString()}`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`API 오류: ${response.status} ${await response.text()}`);
  }

  return response.json() as Promise<AbandonmentApiResponse>;
}
