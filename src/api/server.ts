import express, {Request, Response} from "express";
import {
  CloudinaryUploadConfig,
  parseCloudinaryUrl,
  uploadImageUrlToCloudinary,
} from "../shared/cloudinaryImageUpload.js";
import {runSync} from "../shared/syncAnimalEmbeddingsCore.js";
import {
  backfillShelterInfoToFirestore,
} from "../jobs/backfillShelterInfoToFirestore.js";
import {syncShelterAnimalsStatus} from "../jobs/syncShelterAnimalsStatus.js";

const app = express();
const port = Number(process.env.PORT ?? 8080);

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

app.post("/api/images/upload", async (req: Request, res: Response) => {
  const imageUrl = typeof req.body?.imageUrl === "string" ?
    req.body.imageUrl.trim() :
    "";
  const publicId = typeof req.body?.publicId === "string" ?
    req.body.publicId.trim() :
    undefined;

  if (!imageUrl || !imageUrl.startsWith("http")) {
    res.status(400).json({ok: false, error: "유효한 imageUrl이 필요합니다."});
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

    const uploaded = await uploadImageUrlToCloudinary(
      imageUrl,
      cloudinaryConfig,
      publicId
    );
    res.status(200).json({ok: true, image: uploaded});
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Cloudinary 이미지 업로드 실패:", error);
    res.status(500).json({ok: false, error: message});
  }
});

app.listen(port, "0.0.0.0", () => {
  console.log(`Cloud Run sync server listening on port ${port}`);
});
