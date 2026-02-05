/**
 * Firebase Cloud Functions
 * @see https://firebase.google.com/docs/functions
 */

import {setGlobalOptions} from "firebase-functions";
import {syncAnimalEmbeddings} from "./syncAnimalEmbeddings.js";

setGlobalOptions({maxInstances: 10});

// GCP Cloud Scheduler에 의해 실행: 공공데이터 popfile 벡터화 → Pinecone 저장
export {syncAnimalEmbeddings};
