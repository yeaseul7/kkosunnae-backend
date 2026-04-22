import express, {Request, Response} from "express";
import {
  CloudinaryUploadConfig,
  parseCloudinaryUrl,
  uploadImageUrlToCloudinary,
} from "./cloudinaryImageUpload.js";
import {runSync} from "./syncAnimalEmbeddings.js";

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

app.get("/healthz", (_req: Request, res: Response) => {
  res.status(200).json({ok: true});
});

app.post("/api/sync", async (_req: Request, res: Response) => {
  const serviceKey = process.env.ANIMALS_OPENAPI_KEY;
  const pineconeApiKey = process.env.PINECONE_API_KEY;

  if (!serviceKey) {
    res.status(500).json({
      ok: false,
      error: "ANIMALS_OPENAPI_KEY 환경변수가 설정되지 않았습니다.",
    });
    return;
  }

  if (!pineconeApiKey) {
    res.status(500).json({
      ok: false,
      error: "PINECONE_API_KEY 환경변수가 설정되지 않았습니다.",
    });
    return;
  }

  try {
    const cloudinaryConfig = getCloudinaryConfigFromEnv();
    await runSync(serviceKey, pineconeApiKey, cloudinaryConfig);
    res.status(200).json({ok: true});
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("동물 프로필 벡터화 동기화 실패:", error);
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
