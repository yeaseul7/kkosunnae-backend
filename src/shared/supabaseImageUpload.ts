import {createSupabaseServerClient} from "./shelterAnimalSupabase.js";

const DEFAULT_BUCKET = "board-images";

export interface SupabaseImageUploadResult {
  path: string;
  publicUrl: string;
  originalUrl: string;
  contentType: string;
}

/**
 * public path로 안전하게 쓸 수 있도록 문자열 정규화
 * @param {string} value 원본 식별자
 * @return {string} 안전한 경로 조각
 */
export function sanitizeStoragePathSegment(value: string): string {
  return value
    .trim()
    .replace(/[^A-Za-z0-9/_-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 180);
}

/**
 * 업로드 대상 버킷 이름 조회
 * @return {string} Supabase Storage 버킷명
 */
export function getImageBucketName(): string {
  return process.env.SUPABASE_IMAGE_BUCKET?.trim() || DEFAULT_BUCKET;
}

/**
 * 파일명에서 확장자 추출
 * @param {string} value 파일명 또는 URL
 * @return {string} 확장자
 */
function getExtension(value: string): string {
  const cleaned = value.split("?")[0].split("#")[0];
  const lastDot = cleaned.lastIndexOf(".");
  if (lastDot < 0) return "";
  return cleaned.slice(lastDot).toLowerCase();
}

/**
 * content-type에서 확장자 추정
 * @param {string} contentType MIME 타입
 * @return {string} 확장자
 */
function getExtensionFromContentType(contentType: string): string {
  if (contentType === "image/jpeg") return ".jpg";
  if (contentType === "image/png") return ".png";
  if (contentType === "image/webp") return ".webp";
  if (contentType === "image/gif") return ".gif";
  if (contentType === "image/heic") return ".heic";
  if (contentType === "image/heif") return ".heif";
  return "";
}

/**
 * 업로드 경로 생성
 * @param {string | undefined} publicId 요청된 식별자
 * @param {string | undefined} folder 요청된 폴더
 * @param {string} fallbackName 원본 파일명 또는 URL
 * @param {string} contentType MIME 타입
 * @return {string} storage path
 */
function buildStoragePath(
  publicId: string | undefined,
  folder: string | undefined,
  fallbackName: string,
  contentType: string
): string {
  const safeFolder = sanitizeStoragePathSegment(folder || "").replace(
    /^\/+|\/+$/g,
    ""
  );
  const baseName = sanitizeStoragePathSegment(
    publicId || `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
  ) || `${Date.now()}`;
  const extension =
    getExtension(fallbackName) || getExtensionFromContentType(contentType);
  const filename = `${baseName}${extension}`;
  return safeFolder ? `${safeFolder}/${filename}` : filename;
}

/**
 * Supabase Storage에 이미지 버퍼 업로드
 * @param {Buffer} fileBuffer 업로드할 버퍼
 * @param {string} originalName 원본 파일명
 * @param {string} contentType MIME 타입
 * @param {string | undefined} folder 폴더 경로
 * @param {string | undefined} publicId 요청된 식별자
 * @return {Promise<SupabaseImageUploadResult>} 업로드 결과
 */
export async function uploadImageBufferToSupabaseStorage(
  fileBuffer: Buffer,
  originalName: string,
  contentType: string,
  folder?: string,
  publicId?: string
): Promise<SupabaseImageUploadResult> {
  const supabase = createSupabaseServerClient();
  const bucket = getImageBucketName();
  const path = buildStoragePath(publicId, folder, originalName, contentType);

  const {error} = await supabase.storage
    .from(bucket)
    .upload(path, fileBuffer, {
      contentType,
      upsert: true,
    });

  if (error) {
    throw new Error(`Supabase Storage 업로드 실패 [${bucket}]: ${error.message}`);
  }

  const {data} = supabase.storage.from(bucket).getPublicUrl(path);
  return {
    path,
    publicUrl: data.publicUrl,
    originalUrl: originalName,
    contentType,
  };
}

/**
 * 외부 이미지 URL을 받아 Supabase Storage에 업로드
 * @param {string} imageUrl 원본 이미지 URL
 * @param {string | undefined} folder 폴더 경로
 * @param {string | undefined} publicId 요청된 식별자
 * @return {Promise<SupabaseImageUploadResult>} 업로드 결과
 */
export async function uploadImageUrlToSupabaseStorage(
  imageUrl: string,
  folder?: string,
  publicId?: string
): Promise<SupabaseImageUploadResult> {
  const response = await fetch(imageUrl);
  if (!response.ok) {
    throw new Error(
      `원본 이미지 다운로드 실패: ${response.status} ${response.statusText}`
    );
  }

  const arrayBuffer = await response.arrayBuffer();
  const contentType =
    response.headers.get("content-type") || "application/octet-stream";

  return uploadImageBufferToSupabaseStorage(
    Buffer.from(arrayBuffer),
    imageUrl,
    contentType,
    folder,
    publicId
  );
}
