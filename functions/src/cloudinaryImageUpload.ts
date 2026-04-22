import {v2 as cloudinary} from "cloudinary";
import type {UploadApiResponse} from "cloudinary";

const DEFAULT_FOLDER = "shelter-animals";

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
  cloudinary.config({
    cloud_name: config.cloudName,
    api_key: config.apiKey,
    api_secret: config.apiSecret,
    secure: true,
  });

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
