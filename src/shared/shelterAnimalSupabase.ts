import {createClient} from "@supabase/supabase-js";
import {ShelterAnimalItem} from "./types.js";

export const SUPABASE_ANIMALS_TABLE =
  process.env.SUPABASE_ANIMALS_TABLE?.trim() || "animals";
export const SUPABASE_ANIMALS_CONFLICT_COLUMN =
  process.env.SUPABASE_ANIMALS_CONFLICT_COLUMN?.trim() || "id";
export const SUPABASE_BATCH_SIZE = 200;

export interface SupabaseAnimalRow {
  id: string;
  desertion_no: string;
  care_reg_no: string | null;
  notice_no: string | null;
  srvc_txt: string | null;
  sprt_e_date: string | null;
  rfid_cd: string | null;
  age: string | null;
  weight: string | null;
  color_cd: string | null;
  evnt_img: string | null;
  upd_tm: string | null;
  end_reason: string | null;
  sex_cd: string | null;
  neuter_yn: string | null;
  care_nm: string | null;
  care_tel: string | null;
  care_addr: string | null;
  org_nm: string | null;
  sfe_soci: string | null;
  sfe_health: string | null;
  etc_bigo: string | null;
  up_kind_cd: string | null;
  up_kind_nm: string | null;
  kind_cd: string | null;
  kind_nm: string | null;
  kind_full_nm: string | null;
  process_state: string | null;
  care_owner_nm: string | null;
  vaccination_chk: string | null;
  health_chk: string | null;
  adptn_title: string | null;
  adptn_s_date: string | null;
  adptn_e_date: string | null;
  adptn_condition_limit_txt: string | null;
  adptn_txt: string | null;
  adptn_img: string | null;
  sprt_title: string | null;
  sprt_s_date: string | null;
  sprt_condition_limit_txt: string | null;
  sprt_txt: string | null;
  sprt_img: string | null;
  srvc_title: string | null;
  srvc_s_date: string | null;
  srvc_e_date: string | null;
  srvc_condition_limit_txt: string | null;
  srvc_img: string | null;
  evnt_title: string | null;
  evnt_s_date: string | null;
  evnt_e_date: string | null;
  evnt_condition_limit_txt: string | null;
  evnt_txt: string | null;
  notice_sdt: string | null;
  notice_edt: string | null;
  happen_dt: string | null;
  happen_place: string | null;
  special_mark: string | null;
  popfiles: string[] | null;
  updated_at: string;
}

/**
 * Supabase 환경변수 확인
 * @return {{url: string, key: string}} Supabase 접속 설정
 */
export function getSupabaseConfig(): {url: string; key: string} {
  const url = process.env.SUPABASE_URL?.trim();
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY?.trim();
  if (!url) {
    throw new Error("SUPABASE_URL 환경변수가 설정되지 않았습니다.");
  }
  if (!key) {
    throw new Error("SUPABASE_SERVICE_ROLE_KEY 환경변수가 설정되지 않았습니다.");
  }
  return {url, key};
}

/**
 * Supabase 서버 클라이언트 생성
 * @return {object} Supabase 클라이언트
 */
export function createSupabaseServerClient(): ReturnType<typeof createClient> {
  const {url, key} = getSupabaseConfig();
  return createClient(url, key, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });
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
 * desertionNo 기반 결정적 UUID 생성
 * @param {string} value desertionNo
 * @return {string} UUID 문자열
 */
function createDeterministicUuid(value: string): string {
  let seed = 0;
  const source = `animal:${value}`;
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
 * 축종 코드/품종명에서 축종명 추출
 * @param {ShelterAnimalItem} item 유기동물 항목
 * @return {string | null} 축종명
 */
function getUpKindName(item: ShelterAnimalItem): string | null {
  const upKindName = toNullableTrimmedString(item.upKindNm);
  if (upKindName) return upKindName;

  const kindFullName = toNullableTrimmedString(item.kindCd);
  if (kindFullName?.startsWith("[") && kindFullName.includes("]")) {
    return kindFullName.slice(1, kindFullName.indexOf("]")).trim() || null;
  }

  const upKindCd = toNullableTrimmedString(item.upKindCd);
  if (upKindCd === "417000") return "개";
  if (upKindCd === "422400") return "고양이";
  if (upKindCd === "429900") return "기타";
  return upKindCd;
}

/**
 * 축종 코드/품종명에서 품종명 추출
 * @param {ShelterAnimalItem} item 유기동물 항목
 * @return {string | null} 품종명
 */
function getKindName(item: ShelterAnimalItem): string | null {
  const kindName = toNullableTrimmedString(item.kindNm);
  if (kindName) return kindName;

  const kindFullName = toNullableTrimmedString(item.kindCd);
  if (!kindFullName) return null;
  if (kindFullName.startsWith("[") && kindFullName.includes("]")) {
    return kindFullName.slice(kindFullName.indexOf("]") + 1).trim() || null;
  }
  return kindFullName;
}

/**
 * 유기동물 항목에서 이미지 URL 목록 추출
 * @param {ShelterAnimalItem} item 유기동물 항목
 * @return {string[] | null} 이미지 URL 목록
 */
function getPopfiles(item: ShelterAnimalItem): string[] | null {
  const urls = new Set<string>();
  for (const [key, value] of Object.entries(item)) {
    if (!/^popfile\d*$/.test(key)) continue;
    const url = toNullableTrimmedString(value);
    if (url && url.startsWith("http")) urls.add(url);
  }
  return urls.size > 0 ? [...urls] : null;
}

/**
 * API 항목을 Supabase animals 행으로 변환
 * @param {ShelterAnimalItem} item 유기동물 항목
 * @return {SupabaseAnimalRow | null} Supabase 행 또는 null
 */
export function mapShelterAnimalToSupabaseRow(
  item: ShelterAnimalItem
): SupabaseAnimalRow | null {
  const desertionNo = toNullableTrimmedString(item.desertionNo);
  if (!desertionNo) return null;

  return {
    id: createDeterministicUuid(desertionNo),
    desertion_no: desertionNo,
    care_reg_no: toNullableTrimmedString(item.careRegNo),
    notice_no: toNullableTrimmedString(item.noticeNo),
    srvc_txt: toNullableTrimmedString(item.srvcTxt),
    sprt_e_date: toNullableTrimmedString(item.sprtEDate),
    rfid_cd: toNullableTrimmedString(item.rfidCd),
    age: toNullableTrimmedString(item.age),
    weight: toNullableTrimmedString(item.weight),
    color_cd: toNullableTrimmedString(item.colorCd),
    evnt_img: toNullableTrimmedString(item.evntImg),
    upd_tm: toNullableTrimmedString(item.updTm),
    end_reason: toNullableTrimmedString(item.endReason),
    sex_cd: toNullableTrimmedString(item.sexCd),
    neuter_yn: toNullableTrimmedString(item.neuterYn),
    care_nm: toNullableTrimmedString(item.careNm),
    care_tel: toNullableTrimmedString(item.careTel),
    care_addr: toNullableTrimmedString(item.careAddr),
    org_nm: toNullableTrimmedString(item.orgNm),
    sfe_soci: toNullableTrimmedString(item.sfeSoci),
    sfe_health: toNullableTrimmedString(item.sfeHealth),
    etc_bigo: toNullableTrimmedString(item.etcBigo),
    up_kind_cd: toNullableTrimmedString(item.upKindCd),
    up_kind_nm: getUpKindName(item),
    kind_cd: toNullableTrimmedString(item.kindCd),
    kind_nm: getKindName(item),
    kind_full_nm:
      toNullableTrimmedString(item.kindFullNm) ||
      toNullableTrimmedString(item.kindCd),
    process_state: toNullableTrimmedString(item.processState),
    care_owner_nm: toNullableTrimmedString(item.careOwnerNm),
    vaccination_chk: toNullableTrimmedString(item.vaccinationChk),
    health_chk: toNullableTrimmedString(item.healthChk),
    adptn_title: toNullableTrimmedString(item.adptnTitle),
    adptn_s_date: toNullableTrimmedString(item.adptnSDate),
    adptn_e_date: toNullableTrimmedString(item.adptnEDate),
    adptn_condition_limit_txt:
      toNullableTrimmedString(item.adptnConditionLimitTxt),
    adptn_txt: toNullableTrimmedString(item.adptnTxt),
    adptn_img: toNullableTrimmedString(item.adptnImg),
    sprt_title: toNullableTrimmedString(item.sprtTitle),
    sprt_s_date: toNullableTrimmedString(item.sprtSDate),
    sprt_condition_limit_txt:
      toNullableTrimmedString(item.sprtConditionLimitTxt),
    sprt_txt: toNullableTrimmedString(item.sprtTxt),
    sprt_img: toNullableTrimmedString(item.sprtImg),
    srvc_title: toNullableTrimmedString(item.srvcTitle),
    srvc_s_date: toNullableTrimmedString(item.srvcSDate),
    srvc_e_date: toNullableTrimmedString(item.srvcEDate),
    srvc_condition_limit_txt:
      toNullableTrimmedString(item.srvcConditionLimitTxt),
    srvc_img: toNullableTrimmedString(item.srvcImg),
    evnt_title: toNullableTrimmedString(item.evntTitle),
    evnt_s_date: toNullableTrimmedString(item.evntSDate),
    evnt_e_date: toNullableTrimmedString(item.evntEDate),
    evnt_condition_limit_txt:
      toNullableTrimmedString(item.evntConditionLimitTxt),
    evnt_txt: toNullableTrimmedString(item.evntTxt),
    notice_sdt: toNullableTrimmedString(item.noticeSdt),
    notice_edt: toNullableTrimmedString(item.noticeEdt),
    happen_dt: toNullableTrimmedString(item.happenDt),
    happen_place: toNullableTrimmedString(item.happenPlace),
    special_mark: toNullableTrimmedString(item.specialMark),
    popfiles: getPopfiles(item),
    updated_at: new Date().toISOString(),
  };
}
