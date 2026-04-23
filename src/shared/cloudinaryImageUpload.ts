import {v2 as cloudinary} from "cloudinary";
import type {UploadApiResponse} from "cloudinary";

const DEFAULT_FOLDER = "shelter-animals";
const CLOUDINARY_RESOURCE_PAGE_SIZE = 500;
const CLOUDINARY_DELETE_BATCH_SIZE = 100;

export interface CloudinaryUploadConfig {
  cloudName: string;
  apiKey: string;
  apiSecret: string;
  folder?: string;
}

export interface CloudinaryImageUploadResult {
  publicId: string;
  secureUrl: string;
  originalUrl: string;
  width: number;
  height: number;
  format: string;
}

export interface CloudinaryDeleteResult {
  publicId: string;
  result: string;
}

export interface CloudinaryListResult {
  publicIds: string[];
}

interface CloudinaryResource {
  public_id?: string;
}

interface CloudinaryResourcePage {
  resources?: CloudinaryResource[];
  next_cursor?: string;
}

interface CloudinaryDeleteResourcesResult {
  deleted?: Record<string, string>;
}

/**
 * CLOUDINARY_URL(cloudinary://api_key:api_secret@cloud_name)에서 설정 추출
 * @param {string | undefined} cloudinaryUrl Cloudinary 연결 URL
 * @return {CloudinaryUploadConfig | null} 업로드 설정 또는 null
 */
export function parseCloudinaryUrl(
  cloudinaryUrl: string | undefined
): CloudinaryUploadConfig | null {
  if (!cloudinaryUrl) return null;

  const parsed = new URL(cloudinaryUrl);
  if (parsed.protocol !== "cloudinary:") {
    throw new Error("CLOUDINARY_URL은 cloudinary://로 시작해야 합니다.");
  }

  const cloudName = parsed.hostname;
  const apiKey = decodeURIComponent(parsed.username);
  const apiSecret = decodeURIComponent(parsed.password);

  if (!cloudName || !apiKey || !apiSecret) {
    throw new Error("CLOUDINARY_URL에 cloud_name/api_key/api_secret이 필요합니다.");
  }

  return {
    cloudName,
    apiKey,
    apiSecret,
  };
}

/**
 * Cloudinary public_id로 안전하게 쓸 수 있도록 문자열 정규화
 * @param {string} value 원본 식별자
 * @return {string} Cloudinary public_id 조각
 */
export function sanitizeCloudinaryPublicId(value: string): string {
  return value
    .trim()
    .replace(/[^A-Za-z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
}

/**
 * Cloudinary SDK 인증 설정
 * @param {CloudinaryUploadConfig} config Cloudinary 인증 설정
 */
function configureCloudinary(config: CloudinaryUploadConfig): void {
  cloudinary.config({
    cloud_name: config.cloudName,
    api_key: config.apiKey,
    api_secret: config.apiSecret,
    secure: true,
  });
}

/**
 * 외부 이미지 URL을 Cloudinary에 업로드
 * @param {string} imageUrl 업로드할 원본 이미지 URL
 * @param {CloudinaryUploadConfig} config Cloudinary 인증/폴더 설정
 * @param {string | undefined} publicId 저장할 public_id
 * @return {Promise<CloudinaryImageUploadResult>} 업로드 결과
 */
export async function uploadImageUrlToCloudinary(
  imageUrl: string,
  config: CloudinaryUploadConfig,
  publicId?: string
): Promise<CloudinaryImageUploadResult> {
  configureCloudinary(config);

  const safePublicId = publicId ? sanitizeCloudinaryPublicId(publicId) : "";
  const result: UploadApiResponse = await cloudinary.uploader.upload(imageUrl, {
    folder: config.folder ?? DEFAULT_FOLDER,
    public_id: safePublicId || undefined,
    overwrite: true,
    resource_type: "image",
    unique_filename: !safePublicId,
  });

  return {
    publicId: result.public_id,
    secureUrl: result.secure_url,
    originalUrl: imageUrl,
    width: result.width,
    height: result.height,
    format: result.format,
  };
}

/**
 * 업로드된 파일 버퍼를 Cloudinary에 업로드
 * @param {Buffer} fileBuffer 업로드할 이미지 파일 버퍼
 * @param {string} originalName 원본 파일명
 * @param {CloudinaryUploadConfig} config Cloudinary 인증/폴더 설정
 * @param {string | undefined} publicId 저장할 public_id
 * @return {Promise<CloudinaryImageUploadResult>} 업로드 결과
 */
export async function uploadImageBufferToCloudinary(
  fileBuffer: Buffer,
  originalName: string,
  config: CloudinaryUploadConfig,
  publicId?: string
): Promise<CloudinaryImageUploadResult> {
  configureCloudinary(config);

  const safePublicId = publicId ? sanitizeCloudinaryPublicId(publicId) : "";
  const result = await new Promise<UploadApiResponse>((resolve, reject) => {
    const stream = cloudinary.uploader.upload_stream(
      {
        folder: config.folder ?? DEFAULT_FOLDER,
        public_id: safePublicId || undefined,
        overwrite: true,
        resource_type: "image",
        unique_filename: !safePublicId,
      },
      (error, uploaded) => {
        if (error) {
          reject(error);
          return;
        }

        if (!uploaded) {
          reject(new Error("Cloudinary upload result is empty."));
          return;
        }

        resolve(uploaded);
      }
    );

    stream.end(fileBuffer);
  });

  return {
    publicId: result.public_id,
    secureUrl: result.secure_url,
    originalUrl: originalName,
    width: result.width,
    height: result.height,
    format: result.format,
  };
}

/**
 * Cloudinary 이미지 단건 삭제
 * @param {string} publicId 삭제할 Cloudinary public_id
 * @param {CloudinaryUploadConfig} config Cloudinary 인증 설정
 * @return {Promise<CloudinaryDeleteResult>} 삭제 결과
 */
export async function deleteCloudinaryImage(
  publicId: string,
  config: CloudinaryUploadConfig
): Promise<CloudinaryDeleteResult> {
  configureCloudinary(config);
  const trimmedPublicId = publicId.trim();
  if (!trimmedPublicId) {
    throw new Error("삭제할 publicId가 필요합니다.");
  }

  const result = await cloudinary.uploader.destroy(trimmedPublicId, {
    resource_type: "image",
    invalidate: true,
  });

  return {
    publicId: trimmedPublicId,
    result: String(result.result ?? ""),
  };
}

/**
 * Cloudinary 폴더 안의 이미지 public_id 전체 조회
 * @param {CloudinaryUploadConfig} config Cloudinary 인증/폴더 설정
 * @return {Promise<CloudinaryListResult>} public_id 목록
 */
export async function listCloudinaryImagePublicIds(
  config: CloudinaryUploadConfig
): Promise<CloudinaryListResult> {
  configureCloudinary(config);
  const folder = config.folder ?? DEFAULT_FOLDER;
  const prefix = folder ? `${folder.replace(/\/+$/g, "")}/` : "";
  const publicIds: string[] = [];
  let nextCursor: string | undefined;

  do {
    const page = await cloudinary.api.resources({
      type: "upload",
      resource_type: "image",
      prefix,
      max_results: CLOUDINARY_RESOURCE_PAGE_SIZE,
      next_cursor: nextCursor,
    }) as CloudinaryResourcePage;

    for (const resource of page.resources ?? []) {
      if (resource.public_id) publicIds.push(resource.public_id);
    }

    nextCursor = page.next_cursor;
    console.info(`Cloudinary 이미지 조회 진행: 누적 ${publicIds.length}건`);
  } while (nextCursor);

  return {publicIds};
}

/**
 * Cloudinary 이미지 여러 건 삭제
 * @param {string[]} publicIds 삭제할 Cloudinary public_id 목록
 * @param {CloudinaryUploadConfig} config Cloudinary 인증 설정
 * @return {Promise<number>} 삭제 요청된 이미지 수
 */
export async function deleteCloudinaryImages(
  publicIds: string[],
  config: CloudinaryUploadConfig
): Promise<number> {
  configureCloudinary(config);
  let deleted = 0;

  for (let i = 0; i < publicIds.length; i += CLOUDINARY_DELETE_BATCH_SIZE) {
    const ids = publicIds.slice(i, i + CLOUDINARY_DELETE_BATCH_SIZE);
    const result = await cloudinary.api.delete_resources(ids, {
      resource_type: "image",
      invalidate: true,
    }) as CloudinaryDeleteResourcesResult;
    deleted += Object.keys(result.deleted ?? {}).length || ids.length;
    console.info(
      `Cloudinary 이미지 삭제 진행: ${deleted}/${publicIds.length}`
    );
  }

  return deleted;
}
