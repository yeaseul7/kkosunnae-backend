import {
  fetchAbandonmentApiPage,
  parseShelterItemList,
  SHELTER_API_PAGE_SIZE,
} from "../shared/shelterAnimalFirestoreWrite.js";
import {
  createSupabaseServerClient,
  mapShelterAnimalToSupabaseRow,
  SUPABASE_ANIMALS_CONFLICT_COLUMN,
  SUPABASE_ANIMALS_TABLE,
  SUPABASE_BATCH_SIZE,
  SupabaseAnimalRow,
} from "../shared/shelterAnimalSupabase.js";
import {ShelterAnimalItem} from "../shared/types.js";

export interface SyncShelterAnimalsStatusResult {
  ok: true;
  apiRows: number;
  upserted: number;
  updated: number;
  deleted: number;
  unchanged: number;
}

interface ExistingAnimalRow {
  id: string;
  desertion_no: string;
  process_state: string | null;
  neuter_yn: string | null;
}

interface ExistingShelterRow {
  care_reg_no: string;
}

interface CurrentAnimalsSnapshot {
  stateMap: Map<string, {processState: string; neuterYn: string}>;
  itemList: ShelterAnimalItem[];
}

/**
 * @param {string | undefined} value 원본 문자열
 * @return {string} trim된 문자열
 */
function normalize(value: string | undefined | null): string {
  return String(value ?? "").trim();
}

/**
 * API 전체 페이지를 순회해 현재 유기동물 상태 맵을 생성
 * @param {string} serviceKey 공공데이터 API 키
 * @return {Promise<CurrentAnimalsSnapshot>} 현재 유기동물 스냅샷
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
      const mapped = mapShelterAnimalToSupabaseRow(item);
      if (!mapped) continue;
      stateMap.set(mapped.desertion_no, {
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
 * Supabase animals 기존 상태 전체 조회
 * @return {Promise<ExistingAnimalRow[]>} 기존 유기동물 행 목록
 */
async function listExistingAnimals(): Promise<ExistingAnimalRow[]> {
  const supabase = createSupabaseServerClient();
  const rows: ExistingAnimalRow[] = [];
  let from = 0;
  let hasMore = true;

  while (hasMore) {
    const to = from + SUPABASE_BATCH_SIZE - 1;
    const {data, error} = await supabase
      .from(SUPABASE_ANIMALS_TABLE)
      .select("id, desertion_no, process_state, neuter_yn")
      .order("desertion_no", {ascending: true})
      .range(from, to);

    if (error) {
      throw new Error(
        `Supabase animals 조회 실패 [${SUPABASE_ANIMALS_TABLE}]: ${error.message}`
      );
    }

    const batch = (data ?? []) as ExistingAnimalRow[];
    rows.push(...batch);
    console.info(`Supabase animals 조회 진행: 누적 ${rows.length}건`);

    if (batch.length < SUPABASE_BATCH_SIZE) break;
    from += SUPABASE_BATCH_SIZE;
    hasMore = batch.length === SUPABASE_BATCH_SIZE;
  }

  return rows;
}

/**
 * Supabase shelters의 유효한 care_reg_no 목록 조회
 * @return {Promise<Set<string>>} 유효한 보호소 번호 집합
 */
async function listExistingShelterRegNos(): Promise<Set<string>> {
  const supabase = createSupabaseServerClient();
  const rows: ExistingShelterRow[] = [];
  let from = 0;
  let hasMore = true;

  while (hasMore) {
    const to = from + SUPABASE_BATCH_SIZE - 1;
    const {data, error} = await supabase
      .from("shelters")
      .select("care_reg_no")
      .order("care_reg_no", {ascending: true})
      .range(from, to);

    if (error) {
      throw new Error(`Supabase shelters 조회 실패 [shelters]: ${error.message}`);
    }

    const batch = (data ?? []) as ExistingShelterRow[];
    rows.push(...batch);
    if (batch.length < SUPABASE_BATCH_SIZE) break;
    from += SUPABASE_BATCH_SIZE;
    hasMore = batch.length === SUPABASE_BATCH_SIZE;
  }

  return new Set(rows.map((row) => normalize(row.care_reg_no)));
}

/**
 * animals 외래키 제약에 맞게 care_reg_no 정규화
 * @param {SupabaseAnimalRow[]} rows animals upsert 대상
 * @param {Set<string>} validShelterRegNos 존재하는 보호소 번호 집합
 * @return {object} 정규화 결과
 */
function normalizeAnimalShelterRefs(
  rows: SupabaseAnimalRow[],
  validShelterRegNos: Set<string>
): {rows: SupabaseAnimalRow[]; nulled: number} {
  let nulled = 0;
  const normalizedRows = rows.map((row) => {
    const careRegNo = normalize(row.care_reg_no);
    if (!careRegNo || validShelterRegNos.has(careRegNo)) {
      return row;
    }
    nulled++;
    return {
      ...row,
      care_reg_no: null,
    };
  });

  return {rows: normalizedRows, nulled};
}

/**
 * 유기동물 전체를 Supabase animals 테이블에 upsert
 * @param {ShelterAnimalItem[]} items 유기동물 목록
 * @param {Set<string>} validShelterRegNos 존재하는 보호소 번호 집합
 * @return {Promise<number>} upsert 건수
 */
async function upsertAnimals(
  items: ShelterAnimalItem[],
  validShelterRegNos: Set<string>
): Promise<number> {
  const supabase = createSupabaseServerClient();
  const mappedRows = items
    .map((item) => mapShelterAnimalToSupabaseRow(item))
    .filter((row): row is SupabaseAnimalRow => Boolean(row));
  const {rows, nulled} = normalizeAnimalShelterRefs(
    mappedRows,
    validShelterRegNos
  );

  let upserted = 0;
  for (let i = 0; i < rows.length; i += SUPABASE_BATCH_SIZE) {
    const batch = rows.slice(i, i + SUPABASE_BATCH_SIZE);
    const {error} = await supabase
      .from(SUPABASE_ANIMALS_TABLE)
      .upsert(batch as never[], {
        onConflict: SUPABASE_ANIMALS_CONFLICT_COLUMN,
        ignoreDuplicates: false,
      });

    if (error) {
      throw new Error(
        "Supabase animals upsert 실패 " +
          `[${SUPABASE_ANIMALS_TABLE}]: ${error.message}`
      );
    }
    upserted += batch.length;
  }

  if (nulled > 0) {
    console.info(`animals care_reg_no null 처리: ${nulled}건`);
  }

  return upserted;
}

/**
 * 현재 API에 없는 Supabase animals 행 삭제
 * @param {string[]} ids 삭제할 id 목록
 * @return {Promise<number>} 삭제 건수
 */
async function deleteStaleAnimals(ids: string[]): Promise<number> {
  if (ids.length === 0) return 0;

  const supabase = createSupabaseServerClient();
  let deleted = 0;
  for (let i = 0; i < ids.length; i += SUPABASE_BATCH_SIZE) {
    const batch = ids.slice(i, i + SUPABASE_BATCH_SIZE);
    const {error} = await supabase
      .from(SUPABASE_ANIMALS_TABLE)
      .delete()
      .in("id", batch);

    if (error) {
      throw new Error(
        `Supabase animals 삭제 실패 [${SUPABASE_ANIMALS_TABLE}]: ${error.message}`
      );
    }
    deleted += batch.length;
    console.info(`Supabase animals 삭제 진행: ${deleted}/${ids.length}`);
  }

  return deleted;
}

/**
 * shelterAnimals 현재 API 기준으로 Supabase animals upsert/delete 동기화
 * @param {string} serviceKey 공공데이터 API 키
 * @return {Promise<SyncShelterAnimalsStatusResult>} 동기화 결과
 */
export async function syncShelterAnimalsStatus(
  serviceKey: string
): Promise<SyncShelterAnimalsStatusResult> {
  const current = await fetchCurrentAnimalsMap(serviceKey);
  const existingRows = await listExistingAnimals();
  const validShelterRegNos = await listExistingShelterRegNos();
  const existingMap = new Map(
    existingRows.map((row) => [row.desertion_no, row])
  );

  let updated = 0;
  let unchanged = 0;
  for (const [desertionNo, apiState] of current.stateMap.entries()) {
    const existing = existingMap.get(desertionNo);
    if (!existing) continue;

    const beforeState = normalize(existing.process_state);
    const beforeNeuterYn = normalize(existing.neuter_yn);
    if (
      beforeState === apiState.processState &&
      beforeNeuterYn === apiState.neuterYn
    ) {
      unchanged++;
      continue;
    }
    updated++;
  }

  const staleIds = existingRows
    .filter((row) => !current.stateMap.has(row.desertion_no))
    .map((row) => row.id);

  const upserted = await upsertAnimals(current.itemList, validShelterRegNos);
  const deleted = await deleteStaleAnimals(staleIds);

  console.info(
    `animals 동기화 완료: API ${current.stateMap.size}건, ` +
      `upsert ${upserted}건, 업데이트 ${updated}건, ` +
      `삭제 ${deleted}건, 변경없음 ${unchanged}건`
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
