import {getApps, initializeApp} from "firebase-admin/app";
import {FieldValue, getFirestore} from "firebase-admin/firestore";
import * as logger from "firebase-functions/logger";
import {defineSecret} from "firebase-functions/params";
import {onRequest} from "firebase-functions/v2/https";

const BACKFILL_HTTP_TOKEN = defineSecret("SHELTER_BACKFILL_HTTP_TOKEN");
const SHELTER_COLLECTION = "shelter";
const SHELTER_INFO_COLLECTION = "shelter-info";
const BATCH_SIZE = 200;

if (!getApps().length) {
  initializeApp();
}
const firestore = getFirestore();

/**
 * HTTP 수동 호출: shelter 문서 ID와 shelter-info 문서 ID를 비교해
 * 동일 ID가 있으면 shelter 데이터 병합 후 shelter 문서를 삭제
 */
export const migrateShelterToShelterInfo = onRequest(
  {
    secrets: [BACKFILL_HTTP_TOKEN],
    memory: "1GiB",
    timeoutSeconds: 540,
    cors: false,
  },
  async (req, res) => {
    if (req.method !== "POST") {
      res.status(405).send("Use POST");
      return;
    }

    const expected = BACKFILL_HTTP_TOKEN.value();
    const sent = req.get("x-backfill-token") ?? "";
    if (!expected || sent !== expected) {
      res.status(403).send("Forbidden");
      return;
    }

    try {
      const shelterSnap = await firestore.collection(SHELTER_COLLECTION).get();
      const shelterDocs = shelterSnap.docs;

      let matched = 0;
      let migrated = 0;
      let skipped = 0;

      for (let i = 0; i < shelterDocs.length; i += BATCH_SIZE) {
        const chunk = shelterDocs.slice(i, i + BATCH_SIZE);
        const infoRefs = chunk.map((doc) =>
          firestore.collection(SHELTER_INFO_COLLECTION).doc(doc.id)
        );
        const infoSnaps = await firestore.getAll(...infoRefs);
        const batch = firestore.batch();

        for (let idx = 0; idx < chunk.length; idx++) {
          const shelterDoc = chunk[idx];
          const infoSnap = infoSnaps[idx];
          if (!infoSnap.exists) {
            skipped++;
            continue;
          }

          matched++;
          const shelterData = shelterDoc.data();
          batch.set(infoSnap.ref, {
            shelterMigratedData: shelterData,
            shelterMigratedAt: FieldValue.serverTimestamp(),
          }, {merge: true});
          batch.delete(shelterDoc.ref);
          migrated++;
        }

        await batch.commit();
        const progressed = Math.min(i + BATCH_SIZE, shelterDocs.length);
        const progressText = `${progressed}/${shelterDocs.length}`;
        logger.info(
          `shelter -> shelter-info 이관 진행: ${progressText}, ` +
            `매칭 ${matched}, 삭제 ${migrated}, 스킵 ${skipped}`
        );
      }

      res.status(200).json({
        ok: true,
        sourceCollection: SHELTER_COLLECTION,
        targetCollection: SHELTER_INFO_COLLECTION,
        checked: shelterDocs.length,
        matched,
        migrated,
        skipped,
      });
    } catch (error) {
      logger.error("shelter -> shelter-info 이관 실패:", error);
      res.status(500).json({
        ok: false,
        error: String(error),
      });
    }
  }
);
