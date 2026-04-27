import {createClient, SupabaseClient} from "@supabase/supabase-js";
import {getApps, initializeApp} from "firebase-admin/app";
import {
  FieldPath,
  Firestore,
  Query,
  QueryDocumentSnapshot,
  Timestamp,
  getFirestore,
} from "firebase-admin/firestore";
import {embedImage} from "../shared/imageEmbedding.js";

const FIRESTORE_PAGE_SIZE = 200;
const SUPABASE_UPSERT_BATCH_SIZE = 200;

interface CollectionMigrationConfig {
  label: string;
  sourceCollection: string;
  targetTable: string;
  conflictColumn: string;
  required: boolean;
}

interface MigrationStats {
  scanned: number;
  upserted: number;
  embedded: number;
  skipped: number;
}

type PlainRecord = Record<string, unknown>;

if (!getApps().length) {
  initializeApp();
}

/**
 * 필수 환경변수 조회
 * @param {string} name 환경변수 이름
 * @return {string} trim된 값
 */
function getRequiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} 환경변수가 설정되지 않았습니다.`);
  }
  return value;
}

/**
 * 불리언 환경변수 조회
 * @param {string} name 환경변수 이름
 * @param {boolean} defaultValue 기본값
 * @return {boolean} 파싱 결과
 */
function getBoolEnv(name: string, defaultValue: boolean): boolean {
  const value = process.env[name]?.trim().toLowerCase();
  if (!value) return defaultValue;
  return value === "1" || value === "true" || value === "yes";
}

/**
 * 숫자 환경변수 조회
 * @param {string} name 환경변수 이름
 * @param {number} defaultValue 기본값
 * @return {number} 양의 정수
 */
function getNumberEnv(name: string, defaultValue: number): number {
  const raw = Number(process.env[name] ?? defaultValue);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : defaultValue;
}

/**
 * 단일 컬렉션 라벨 필터 조회
 * @return {string | null} users/shelters/animals 또는 null
 */
function getOnlyLabel(): string | null {
  const value = process.env.MIGRATION_ONLY?.trim().toLowerCase();
  return value ? value : null;
}

/**
 * Firestore 값을 JSON 직렬화 가능한 값으로 정규화
 * @param {unknown} value Firestore 원본 값
 * @return {unknown} 정규화된 값
 */
function normalizeValue(value: unknown): unknown {
  if (value instanceof Timestamp) {
    return value.toDate().toISOString();
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeValue(item));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .map(([key, innerValue]) => [key, normalizeValue(innerValue)])
    );
  }
  return value;
}

/**
 * Firestore 문서를 plain object로 변환
 * @param {QueryDocumentSnapshot} snapshot Firestore 문서
 * @return {PlainRecord} 일반 객체
 */
function toPlainRecord(snapshot: QueryDocumentSnapshot): PlainRecord {
  const data = normalizeValue(snapshot.data()) as PlainRecord;
  return {
    id: snapshot.id,
    docId: snapshot.id,
    ...data,
  };
}

/**
 * 컬렉션/테이블 매핑 설정
 * @return {CollectionMigrationConfig[]} 마이그레이션 설정 목록
 */
function getCollectionConfigs(): CollectionMigrationConfig[] {
  return [
    {
      label: "users",
      sourceCollection:
        process.env.MIGRATION_USERS_COLLECTION?.trim() || "users",
      targetTable: process.env.SUPABASE_USERS_TABLE?.trim() || "users",
      conflictColumn:
        process.env.SUPABASE_USERS_CONFLICT_COLUMN?.trim() || "id",
      required: false,
    },
    {
      label: "animals",
      sourceCollection:
        process.env.MIGRATION_ANIMALS_COLLECTION?.trim() || "shelterAnimals",
      targetTable: process.env.SUPABASE_ANIMALS_TABLE?.trim() || "animals",
      conflictColumn:
        process.env.SUPABASE_ANIMALS_CONFLICT_COLUMN?.trim() || "id",
      required: true,
    },
  ];
}

/**
 * Firestore에 존재하는 컬렉션 이름 집합 조회
 * @param {Firestore} firestore Firestore 인스턴스
 * @return {Promise<Set<string>>} 컬렉션 이름 집합
 */
async function getExistingCollectionNames(
  firestore: Firestore
): Promise<Set<string>> {
  const collections = await firestore.listCollections();
  return new Set(collections.map((collection) => collection.id));
}

/**
 * Firestore 페이지 조회 쿼리 생성
 * @param {Firestore} firestore Firestore 인스턴스
 * @param {string} collectionName 컬렉션 이름
 * @param {QueryDocumentSnapshot | undefined} lastDoc 마지막 문서
 * @return {Query} 페이지 쿼리
 */
function buildPagedQuery(
  firestore: Firestore,
  collectionName: string,
  lastDoc?: QueryDocumentSnapshot
): Query {
  let query = firestore
    .collection(collectionName)
    .orderBy(FieldPath.documentId())
    .limit(FIRESTORE_PAGE_SIZE);
  if (lastDoc) {
    query = query.startAfter(lastDoc);
  }
  return query;
}

/**
 * Supabase upsert 실행
 * @param {SupabaseClient} supabase Supabase 클라이언트
 * @param {string} table 대상 테이블
 * @param {string} conflictColumn upsert 충돌 키
 * @param {PlainRecord[]} rows 저장할 행
 * @param {boolean} dryRun dry-run 여부
 * @return {Promise<number>} 처리 건수
 */
async function upsertRows(
  supabase: SupabaseClient,
  table: string,
  conflictColumn: string,
  rows: PlainRecord[],
  dryRun: boolean
): Promise<number> {
  if (rows.length === 0) return 0;
  if (dryRun) return rows.length;

  const {error} = await supabase
    .from(table)
    .upsert(rows, {
      onConflict: conflictColumn,
      ignoreDuplicates: false,
    });

  if (error) {
    throw new Error(`Supabase upsert 실패 [${table}]: ${error.message}`);
  }

  return rows.length;
}

/**
 * 일반 컬렉션 마이그레이션
 * @param {Firestore} firestore Firestore 인스턴스
 * @param {SupabaseClient} supabase Supabase 클라이언트
 * @param {CollectionMigrationConfig} config 컬렉션 설정
 * @param {boolean} dryRun dry-run 여부
 * @param {number} limit 최대 처리 수
 * @return {Promise<MigrationStats>} 처리 통계
 */
async function migratePlainCollection(
  firestore: Firestore,
  supabase: SupabaseClient,
  config: CollectionMigrationConfig,
  dryRun: boolean,
  limit: number
): Promise<MigrationStats> {
  let scanned = 0;
  let upserted = 0;
  let skipped = 0;
  let lastDoc: QueryDocumentSnapshot | undefined;
  let hasMore = true;

  while (hasMore && scanned < limit) {
    const snapshot = await buildPagedQuery(
      firestore,
      config.sourceCollection,
      lastDoc
    ).get();
    if (snapshot.empty) break;

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    const remaining = limit - scanned;
    const docs = snapshot.docs.slice(0, remaining);
    const rows = docs.map((doc) => toPlainRecord(doc));
    upserted += await upsertRows(
      supabase,
      config.targetTable,
      config.conflictColumn,
      rows,
      dryRun
    );
    scanned += docs.length;

    console.info(
      `${config.label} 마이그레이션 진행: scanned ${scanned}, upserted ${upserted}`
    );

    hasMore =
      snapshot.docs.length === FIRESTORE_PAGE_SIZE &&
      scanned < limit;
    if (docs.length < snapshot.docs.length) {
      skipped += snapshot.docs.length - docs.length;
      hasMore = false;
    }
  }

  return {scanned, upserted, embedded: 0, skipped};
}

/**
 * 유기동물 문서에서 임베딩 대상 이미지 URL 추출
 * @param {PlainRecord} record 유기동물 문서
 * @return {string} 이미지 URL 또는 빈 문자열
 */
function getAnimalImageUrl(record: PlainRecord): string {
  const candidates = [
    record.popfile,
    record.popfile1,
    record.imageUrl,
    record.filename,
  ];
  for (const candidate of candidates) {
    const value = String(candidate ?? "").trim();
    if (value.startsWith("http")) return value;
  }
  return "";
}

/**
 * 유기동물 컬렉션 마이그레이션
 * @param {Firestore} firestore Firestore 인스턴스
 * @param {SupabaseClient} supabase Supabase 클라이언트
 * @param {CollectionMigrationConfig} config 컬렉션 설정
 * @param {boolean} dryRun dry-run 여부
 * @param {number} limit 최대 처리 수
 * @return {Promise<MigrationStats>} 처리 통계
 */
async function migrateAnimals(
  firestore: Firestore,
  supabase: SupabaseClient,
  config: CollectionMigrationConfig,
  dryRun: boolean,
  limit: number
): Promise<MigrationStats> {
  const embedColumn =
    process.env.SUPABASE_ANIMALS_EMBED_COLUMN?.trim() || "embedding";
  const enableEmbedding = getBoolEnv("MIGRATION_ENABLE_EMBEDDING", true);

  let scanned = 0;
  let upserted = 0;
  let embedded = 0;
  let skipped = 0;
  let lastDoc: QueryDocumentSnapshot | undefined;
  let hasMore = true;

  while (hasMore && scanned < limit) {
    const snapshot = await buildPagedQuery(
      firestore,
      config.sourceCollection,
      lastDoc
    ).get();
    if (snapshot.empty) break;

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    const remaining = limit - scanned;
    const docs = snapshot.docs.slice(0, remaining);
    const rows: PlainRecord[] = [];

    for (const doc of docs) {
      const record = toPlainRecord(doc);
      const imageUrl = getAnimalImageUrl(record);

      if (enableEmbedding && imageUrl) {
        try {
          const vector = await embedImage(imageUrl);
          if (vector) {
            record[embedColumn] = vector;
            embedded++;
          }
        } catch (error) {
          console.error(`동물 임베딩 실패: ${doc.id}`, error);
        }
      }

      rows.push(record);
    }

    for (let i = 0; i < rows.length; i += SUPABASE_UPSERT_BATCH_SIZE) {
      const batch = rows.slice(i, i + SUPABASE_UPSERT_BATCH_SIZE);
      upserted += await upsertRows(
        supabase,
        config.targetTable,
        config.conflictColumn,
        batch,
        dryRun
      );
    }

    scanned += docs.length;
    console.info(
      "animals 마이그레이션 진행: " +
        `scanned ${scanned}, upserted ${upserted}, embedded ${embedded}`
    );

    hasMore =
      snapshot.docs.length === FIRESTORE_PAGE_SIZE &&
      scanned < limit;
    if (docs.length < snapshot.docs.length) {
      skipped += snapshot.docs.length - docs.length;
      hasMore = false;
    }
  }

  return {scanned, upserted, embedded, skipped};
}

/**
 * Firestore -> Supabase 마이그레이션 메인 함수
 * @return {Promise<void>} 완료 시 resolve
 */
async function main(): Promise<void> {
  const supabaseUrl = getRequiredEnv("SUPABASE_URL");
  const supabaseServiceRoleKey = getRequiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const dryRun = getBoolEnv("MIGRATION_DRY_RUN", true);
  const limit = getNumberEnv("MIGRATION_LIMIT", Number.MAX_SAFE_INTEGER);
  const onlyLabel = getOnlyLabel();

  const firestore = getFirestore();
  const supabase = createClient(supabaseUrl, supabaseServiceRoleKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });
  const existingCollections = await getExistingCollectionNames(firestore);
  const configs = getCollectionConfigs().filter((config) =>
    !onlyLabel || config.label === onlyLabel
  );

  if (configs.length === 0) {
    throw new Error(
      "MIGRATION_ONLY 값이 올바르지 않습니다. " +
        "users, shelters, animals 중 하나를 사용하세요."
    );
  }

  for (const config of configs) {
    if (!existingCollections.has(config.sourceCollection)) {
      if (config.required) {
        throw new Error(
          `필수 Firestore 컬렉션이 없습니다: ${config.sourceCollection}`
        );
      }
      console.info(
        `${config.label} 컬렉션 생략: ${config.sourceCollection} 없음`
      );
      continue;
    }

    const stats = config.label === "animals" ?
      await migrateAnimals(firestore, supabase, config, dryRun, limit) :
      await migratePlainCollection(
        firestore,
        supabase,
        config,
        dryRun,
        limit
      );

    console.info(
      `${config.label} 마이그레이션 완료: ` +
        `scanned ${stats.scanned}, ` +
        `upserted ${stats.upserted}, ` +
        `embedded ${stats.embedded}, ` +
        `skipped ${stats.skipped}, ` +
        `dryRun ${dryRun}`
    );
  }
}

main().catch((error) => {
  console.error("Firestore -> Supabase 마이그레이션 실패:", error);
  process.exit(1);
});
