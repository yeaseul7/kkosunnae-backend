import {pipeline} from "@huggingface/transformers";

export const IMAGE_MODEL_ID = "Xenova/dinov2-small";
export const IMAGE_EMBED_DIM = 384;
export const PINECONE_INDEX_NAME = "embeded-animal";

type ImageInput = string | Blob;

export type FeatureExtractor = (
  input: ImageInput,
  opts?: {pool?: boolean}
) => Promise<{data: Float32Array; dims?: number[]}>;

let cachedExtractor: FeatureExtractor | null = null;
let extractorPromise: Promise<FeatureExtractor> | null = null;

/**
 * DINOv2 이미지 임베딩 모델을 싱글톤으로 로드
 * @return {Promise<FeatureExtractor>} 이미지 feature extractor
 */
export async function loadImageEmbeddingModel(): Promise<FeatureExtractor> {
  if (cachedExtractor) return cachedExtractor;
  if (extractorPromise) return extractorPromise;

  extractorPromise = (async () => {
    const pipe = await pipeline("image-feature-extraction", IMAGE_MODEL_ID);
    cachedExtractor = pipe as unknown as FeatureExtractor;
    return cachedExtractor;
  })();

  return extractorPromise;
}

/**
 * DINOv2 결과에서 CLS 벡터 추출 후 L2 정규화
 * @param {object | null} result 모델 출력
 * @return {number[] | null} 정규화된 384차원 벡터
 */
export function getImageEmbeddingFromResult(
  result: {data?: Float32Array; dims?: number[]} | null
): number[] | null {
  if (!result?.data) return null;

  const data = result.data as Float32Array;
  const dims = result.dims ?? [];
  const hiddenDim = dims.length >= 3 ? dims[2] : data.length;
  const clsVector = data.subarray(0, hiddenDim);

  const arr = Array.from(clsVector);
  const norm = Math.sqrt(arr.reduce((s, x) => s + x * x, 0)) || 1;
  return arr.map((x) => x / norm);
}

/**
 * 이미지 URL 또는 Blob을 벡터화
 * @param {ImageInput} input 이미지 URL 또는 Blob
 * @return {Promise<number[] | null>} 정규화된 임베딩 벡터
 */
export async function embedImage(input: ImageInput): Promise<number[] | null> {
  const extractor = await loadImageEmbeddingModel();
  const result = await extractor(input, {pool: false});
  return getImageEmbeddingFromResult(result);
}
