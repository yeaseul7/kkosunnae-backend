/**
 * Firebase Cloud Functions
 * @see https://firebase.google.com/docs/functions
 */

import {setGlobalOptions} from "firebase-functions";
import {
  backfillShelterAnimalsToFirestore,
} from "./backfillShelterAnimalsToFirestore.js";
import {
  backfillShelterInfoToFirestore,
} from "./backfillShelterInfoToFirestore.js";
import {migrateShelterToShelterInfo} from "./migrateShelterToShelterInfo.js";
import {syncAnimalEmbeddings} from "./syncAnimalEmbeddings.js";
import {syncShelterAnimalsStatus} from "./syncShelterAnimalsStatus.js";

setGlobalOptions({maxInstances: 10});

// GCP Cloud Scheduler: 오늘 유기분 Firestore + Pinecone 임베딩
export {syncAnimalEmbeddings};
// GCP Cloud Scheduler: shelterAnimals 상태/중성화 증분 동기화 + 삭제
export {syncShelterAnimalsStatus};
// HTTP POST: 과거분 Firestore만 20페이지×500건 백필 (헤더 x-backfill-token)
export {backfillShelterAnimalsToFirestore};
// HTTP POST: 보호소 정보 20페이지×500건 Firestore 백필 (헤더 x-backfill-token)
export {backfillShelterInfoToFirestore};
// HTTP POST: shelter -> shelter-info 이관 후 shelter 문서 삭제
export {migrateShelterToShelterInfo};
