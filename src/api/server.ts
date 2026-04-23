import express, {Request, Response} from "express";
import multer from "multer";
import {
  CloudinaryUploadConfig,
  parseCloudinaryUrl,
  uploadImageBufferToCloudinary,
  uploadImageUrlToCloudinary,
} from "../shared/cloudinaryImageUpload.js";
import {runSync} from "../shared/syncAnimalEmbeddingsCore.js";
import {
  backfillShelterInfoToFirestore,
} from "../jobs/backfillShelterInfoToFirestore.js";
import {cleanupPineconeEmbeddings} from "../jobs/cleanupPineconeEmbeddings.js";
import {searchSimilarAnimals} from "../jobs/searchSimilarAnimals.js";
import {syncShelterAnimalsStatus} from "../jobs/syncShelterAnimalsStatus.js";

const app = express();
const port = Number(process.env.PORT ?? 8080);
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024,
    files: 1,
  },
});

app.use((req: Request, res: Response, next) => {
  const origin = req.get("origin");
  res.setHeader("Access-Control-Allow-Origin", origin || "*");
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");

  if (req.method === "OPTIONS") {
    res.status(204).send();
    return;
  }

  next();
});
app.use(express.json());

/**
 * Cloud Run 환경변수에서 Cloudinary 설정을 읽음
 * @return {CloudinaryUploadConfig | null} Cloudinary 업로드 설정
 */
function getCloudinaryConfigFromEnv(): CloudinaryUploadConfig | null {
  const config = parseCloudinaryUrl(process.env.CLOUDINARY_URL);
  if (!config) return null;

  return {
    ...config,
    folder: process.env.CLOUDINARY_FOLDER || config.folder,
  };
}

/**
 * Cloud Scheduler 호출 인증 토큰을 확인
 * @param {Request} req Express 요청
 * @param {Response} res Express 응답
 * @return {boolean} 인증 통과 여부
 */
function verifySchedulerRequest(req: Request, res: Response): boolean {
  const expected = process.env.SCHEDULER_HTTP_TOKEN ??
    process.env.SHELTER_BACKFILL_HTTP_TOKEN;
  if (!expected) return true;

  const auth = req.get("authorization") ?? "";
  const bearer = auth.toLowerCase().startsWith("bearer ") ?
    auth.slice("bearer ".length) :
    "";
  const sent = req.get("x-scheduler-token") ??
    req.get("x-backfill-token") ??
    bearer;

  if (sent === expected) return true;

  res.status(403).json({ok: false, error: "Forbidden"});
  return false;
}

/**
 * 공공데이터 API 키를 환경변수에서 읽음
 * @return {string} 공공데이터 API 키
 */
function getAnimalsOpenApiKey(): string {
  const serviceKey = process.env.ANIMALS_OPENAPI_KEY;
  if (!serviceKey) {
    throw new Error("ANIMALS_OPENAPI_KEY 환경변수가 설정되지 않았습니다.");
  }
  return serviceKey;
}

app.get("/healthz", (_req: Request, res: Response) => {
  res.status(200).json({ok: true});
});

app.post("/api/sync", async (req: Request, res: Response) => {
  if (!verifySchedulerRequest(req, res)) return;

  const pineconeApiKey = process.env.PINECONE_API_KEY;

  if (!pineconeApiKey) {
    res.status(500).json({
      ok: false,
      error: "PINECONE_API_KEY 환경변수가 설정되지 않았습니다.",
    });
    return;
  }

  try {
    const serviceKey = getAnimalsOpenApiKey();
    await runSync(serviceKey, pineconeApiKey);
    res.status(200).json({ok: true});
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("동물 프로필 벡터화 동기화 실패:", error);
    res.status(500).json({ok: false, error: message});
  }
});

app.post("/api/sync/status", async (req: Request, res: Response) => {
  if (!verifySchedulerRequest(req, res)) return;

  try {
    const result = await syncShelterAnimalsStatus(getAnimalsOpenApiKey());
    res.status(200).json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("shelterAnimals 상태 동기화 실패:", error);
    res.status(500).json({ok: false, error: message});
  }
});

app.post("/api/sync/pinecone-cleanup", async (
  req: Request,
  res: Response
) => {
  if (!verifySchedulerRequest(req, res)) return;

  const pineconeApiKey = process.env.PINECONE_API_KEY;
  if (!pineconeApiKey) {
    res.status(500).json({
      ok: false,
      error: "PINECONE_API_KEY 환경변수가 설정되지 않았습니다.",
    });
    return;
  }

  const dryRun =
    req.query.dryRun === "true" ||
    req.body?.dryRun === true ||
    req.body?.dryRun === "true";

  try {
    const result = await cleanupPineconeEmbeddings(
      getAnimalsOpenApiKey(),
      pineconeApiKey,
      {dryRun}
    );
    res.status(200).json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Pinecone stale vector 정리 실패:", error);
    res.status(500).json({ok: false, error: message});
  }
});

app.post("/api/backfill/shelters", async (req: Request, res: Response) => {
  if (!verifySchedulerRequest(req, res)) return;

  try {
    const result = await backfillShelterInfoToFirestore(getAnimalsOpenApiKey());
    res.status(200).json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("보호소 백필 실패:", error);
    res.status(500).json({ok: false, error: message});
  }
});

app.post("/api/images/upload", upload.single("file"), async (
  req: Request,
  res: Response
) => {
  const file = req.file;
  const imageUrl = typeof req.body?.imageUrl === "string" ?
    req.body.imageUrl.trim() :
    "";
  const publicId = typeof req.body?.publicId === "string" ?
    req.body.publicId.trim() :
    undefined;
  const folder = typeof req.body?.folder === "string" ?
    req.body.folder.trim() :
    "";

  if (!file && (!imageUrl || !imageUrl.startsWith("http"))) {
    res.status(400).json({
      ok: false,
      error: "업로드할 file 또는 유효한 imageUrl이 필요합니다.",
    });
    return;
  }

  try {
    const cloudinaryConfig = getCloudinaryConfigFromEnv();
    if (!cloudinaryConfig) {
      res.status(500).json({
        ok: false,
        error: "CLOUDINARY_URL 환경변수가 설정되지 않았습니다.",
      });
      return;
    }

    const uploadConfig = {
      ...cloudinaryConfig,
      folder: folder || cloudinaryConfig.folder,
    };
    const uploaded = file ?
      await uploadImageBufferToCloudinary(
        file.buffer,
        file.originalname,
        uploadConfig,
        publicId
      ) :
      await uploadImageUrlToCloudinary(imageUrl, uploadConfig, publicId);

    res.status(200).json({
      ok: true,
      image: uploaded,
      url: uploaded.secureUrl,
      publicId: uploaded.publicId,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Cloudinary 이미지 업로드 실패:", error);
    res.status(500).json({ok: false, error: message});
  }
});

app.post("/api/search/animals", upload.single("file"), async (
  req: Request,
  res: Response
) => {
  const pineconeApiKey = process.env.PINECONE_API_KEY;
  if (!pineconeApiKey) {
    res.status(500).json({
      ok: false,
      error: "PINECONE_API_KEY 환경변수가 설정되지 않았습니다.",
    });
    return;
  }

  const file = req.file;
  const imageUrl = typeof req.body?.imageUrl === "string" ?
    req.body.imageUrl.trim() :
    "";
  const requestedTopK = Number(req.body?.topK ?? 10);
  const topK = Math.min(50, Math.max(1, Math.floor(requestedTopK) || 10));

  if (!file && (!imageUrl || !imageUrl.startsWith("http"))) {
    res.status(400).json({
      ok: false,
      error: "검색할 file 또는 유효한 imageUrl이 필요합니다.",
    });
    return;
  }

  try {
    const image = file ?
      new Blob([new Uint8Array(file.buffer)], {
        type: file.mimetype || "application/octet-stream",
      }) :
      imageUrl;
    const result = await searchSimilarAnimals(image, pineconeApiKey, topK);
    res.status(200).json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("유사 동물 이미지 검색 실패:", error);
    res.status(500).json({ok: false, error: message});
  }
});

app.listen(port, "0.0.0.0", () => {
  console.log(`Cloud Run sync server listening on port ${port}`);
});
