import {createClient} from "@supabase/supabase-js";

const SHELTERS_API_BASE_URL =
  "https://apis.data.go.kr/1543061/animalShelterSrvc_v2";
const SHELTER_INFO_NUM_OF_ROWS = 500;
const SHELTER_INFO_MAX_PAGES = 20;
const SUPABASE_UPSERT_BATCH_SIZE = 200;
const BETWEEN_PAGES_MS = 400;
const SUPABASE_SHELTERS_TABLE = "shelters";

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
  rowsPerPage: number;
  targetTable: string;
  upsertedRows: number;
}

type SupabaseShelterRow = Record<string, unknown>;

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
 * 빈 문자열을 null로 정규화
 * @param {unknown} value 원본 값
 * @return {string | null} trim 결과 또는 null
 */
function toNullableTrimmedString(value: unknown): string | null {
  const trimmed = String(value ?? "").trim();
  return trimmed ? trimmed : null;
}

/**
 * 숫자형 값을 number로 정규화
 * @param {unknown} value 원본 값
 * @return {number | null} 숫자 또는 null
 */
function toNullableNumber(value: unknown): number | null {
  if (value === undefined || value === null || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

/**
 * careRegNo 기반 결정적 UUID 생성
 * @param {string} value careRegNo
 * @return {string} UUID 문자열
 */
function createDeterministicUuid(value: string): string {
  let seed = 0;
  const source = `shelter:${value}`;
  for (let i = 0; i < source.length; i++) {
    seed = Math.imul(seed ^ source.charCodeAt(i), 2654435761) >>> 0;
  }

  let hex = "";
  let state = seed || 1;
  while (hex.length < 32) {
    state = (Math.imul(state ^ 0x9e3779b9, 1664525) + 1013904223) >>> 0;
    hex += state.toString(16).padStart(8, "0");
  }

  const chars = hex.slice(0, 32).split("");
  chars[12] = "5";
  chars[16] = ((parseInt(chars[16], 16) & 0x3) | 0x8).toString(16);

  return [
    chars.slice(0, 8).join(""),
    chars.slice(8, 12).join(""),
    chars.slice(12, 16).join(""),
    chars.slice(16, 20).join(""),
    chars.slice(20, 32).join(""),
  ].join("-");
}

/**
 * API item을 Supabase shelters 행으로 변환
 * @param {ShelterInfoItem} item 보호소 항목
 * @return {SupabaseShelterRow | null} shelters 행 또는 null
 */
function mapShelterItemToSupabaseRow(
  item: ShelterInfoItem
): SupabaseShelterRow | null {
  const careRegNo = String(item.careRegNo ?? "").trim();
  const careNm = String(item.careNm ?? "").trim();
  if (!careRegNo || !careNm) return null;

  return {
    id: createDeterministicUuid(careRegNo),
    care_reg_no: careRegNo,
    care_nm: careNm,
    care_addr: toNullableTrimmedString(item.careAddr),
    jibun_addr: toNullableTrimmedString(item.jibunAddr),
    lat: toNullableNumber(item.lat),
    lng: toNullableNumber(item.lng),
    care_tel: toNullableTrimmedString(item.careTel),
    close_day: toNullableTrimmedString(item.closeDay),
    week_opr_stime: toNullableTrimmedString(item.weekOprStime),
    week_opr_etime: toNullableTrimmedString(item.weekOprEtime),
    weekend_opr_stime: toNullableTrimmedString(item.weekendOprStime),
    weekend_opr_etime: toNullableTrimmedString(item.weekendOprEtime),
    breed_cnt: toNullableNumber(item.breedCnt),
    vet_person_cnt: toNullableNumber(item.vetPersonCnt),
    specs_person_cnt: toNullableNumber(item.specsPersonCnt),
    medical_cnt: toNullableNumber(item.medicalCnt),
    author_id: toNullableTrimmedString(item.authorId),
    content: toNullableTrimmedString(item.content),
    save_trgt_animal: toNullableTrimmedString(item.saveTrgtAnimal),
    division_nm: toNullableTrimmedString(item.divisionNm),
    org_nm: toNullableTrimmedString(item.orgNm),
    shelter_migrated_at: new Date().toISOString(),
    shelter_migrated_data: item,
    updated_at: new Date().toISOString(),
  };
}

/**
 * Supabase 환경변수 확인
 * @return {{url: string, key: string, table: string}} 설정값
 */
function getSupabaseConfig(): {url: string; key: string; table: string} {
  const url = process.env.SUPABASE_URL?.trim();
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY?.trim();

  if (!url) {
    throw new Error("SUPABASE_URL 환경변수가 설정되지 않았습니다.");
  }
  if (!key) {
    throw new Error("SUPABASE_SERVICE_ROLE_KEY 환경변수가 설정되지 않았습니다.");
  }

  return {url, key, table: SUPABASE_SHELTERS_TABLE};
}

/**
 * 보호소 행 목록을 Supabase에 upsert
 * @param {SupabaseShelterRow[]} rows shelters 행 목록
 * @param {string} table 대상 테이블
 * @return {Promise<number>} upsert 건수
 */
async function upsertShelterRows(
  rows: SupabaseShelterRow[],
  table: string
): Promise<number> {
  if (rows.length === 0) return 0;

  const {url, key} = getSupabaseConfig();
  const supabase = createClient(url, key, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });

  let upserted = 0;
  for (let i = 0; i < rows.length; i += SUPABASE_UPSERT_BATCH_SIZE) {
    const batch = rows.slice(i, i + SUPABASE_UPSERT_BATCH_SIZE);
    const {error} = await supabase.from(table).upsert(batch, {
      onConflict: "care_reg_no",
      ignoreDuplicates: false,
    });
    if (error) {
      throw new Error(`Supabase upsert 실패 [${table}]: ${error.message}`);
    }
    upserted += batch.length;
  }

  return upserted;
}

/**
 * shelterInfo_v2 전체를 조회 후 Supabase shelters 테이블에 증분 저장
 * @param {string} serviceKey 공공데이터 API 키
 * @return {Promise<BackfillShelterInfoResult>} 백필 결과
 */
export async function backfillShelterInfoToSupabase(
  serviceKey: string
): Promise<BackfillShelterInfoResult> {
  const {table} = getSupabaseConfig();
  let pagesFetched = 0;
  let totalUpserted = 0;

  for (let pageNo = 1; pageNo <= SHELTER_INFO_MAX_PAGES; pageNo++) {
    const data = await fetchShelterInfoPage(serviceKey, pageNo);
    const items = parseItems(data);
    const totalCount = data?.response?.body?.totalCount ?? 0;
    const rows = items
      .map((item) => mapShelterItemToSupabaseRow(item))
      .filter((row): row is SupabaseShelterRow => Boolean(row));
    const upserted = await upsertShelterRows(rows, table);
    pagesFetched = pageNo;
    totalUpserted += upserted;

    console.info(
      `보호소 백필 page ${pageNo}/${SHELTER_INFO_MAX_PAGES}: ` +
        `응답 ${items.length}건, Supabase ${upserted}건 ` +
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
      `targetTable ${table}, upsertedRows ${totalUpserted}`
  );

  return {
    ok: true,
    pagesFetched,
    rowsPerPage: SHELTER_INFO_NUM_OF_ROWS,
    targetTable: table,
    upsertedRows: totalUpserted,
  };
}
