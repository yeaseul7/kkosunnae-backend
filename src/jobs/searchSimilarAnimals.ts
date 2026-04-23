import {Pinecone} from "@pinecone-database/pinecone";
import {
  PINECONE_INDEX_NAME,
  embedImage,
} from "../shared/imageEmbedding.js";

export interface SimilarAnimalMatch {
  id: string;
  score?: number;
  metadata?: Record<string, unknown>;
}

export interface SearchSimilarAnimalsResult {
  ok: true;
  topK: number;
  matches: SimilarAnimalMatch[];
}

/**
 * 업로드 이미지와 유사한 Pinecone item 검색
 * @param {string | Blob} image 이미지 URL 또는 Blob
 * @param {string} pineconeApiKey Pinecone API 키
 * @param {number} topK 반환할 최대 결과 수
 * @return {Promise<SearchSimilarAnimalsResult>} 유사도 검색 결과
 */
export async function searchSimilarAnimals(
  image: string | Blob,
  pineconeApiKey: string,
  topK: number
): Promise<SearchSimilarAnimalsResult> {
  const vector = await embedImage(image);
  if (!vector) {
    throw new Error("이미지 임베딩 생성에 실패했습니다.");
  }

  const pc = new Pinecone({apiKey: pineconeApiKey});
  const index = pc.index(PINECONE_INDEX_NAME);
  const result = await index.query({
    vector,
    topK,
    includeMetadata: true,
  });

  return {
    ok: true,
    topK,
    matches: (result.matches ?? []).map((match) => ({
      id: match.id,
      score: match.score,
      metadata: match.metadata as Record<string, unknown> | undefined,
    })),
  };
}
