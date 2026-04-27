export interface ShelterAnimalItem {
  desertionNo?: string;
  noticeNo?: string;
  srvcTxt?: string;
  popfile1?: string;
  popfile2?: string;
  popfile3?: string;
  popfile4?: string;
  popfile5?: string;
  popfile6?: string;
  popfile7?: string;
  popfile8?: string;
  sprtEDate?: string;
  rfidCd?: string;
  filename?: string;
  happenDt?: string;
  happenPlace?: string;
  kindCd?: string;
  colorCd?: string;
  age?: string;
  weight?: string;
  evntImg?: string;
  updTm?: string;
  endReason?: string;
  careRegNo?: string;
  noticeSdt?: string;
  noticeEdt?: string;
  processState?: string;
  sexCd?: string;
  neuterYn?: string;
  specialMark?: string;
  careNm?: string;
  careTel?: string;
  careAddr?: string;
  orgNm?: string;
  sfeSoci?: string;
  sfeHealth?: string;
  etcBigo?: string;
  kindFullNm?: string;
  upKindCd?: string;
  upKindNm?: string;
  kindNm?: string;
  careOwnerNm?: string;
  vaccinationChk?: string;
  healthChk?: string;
  adptnTitle?: string;
  adptnSDate?: string;
  adptnEDate?: string;
  adptnConditionLimitTxt?: string;
  adptnTxt?: string;
  adptnImg?: string;
  sprtTitle?: string;
  sprtSDate?: string;
  sprtConditionLimitTxt?: string;
  sprtTxt?: string;
  sprtImg?: string;
  srvcTitle?: string;
  srvcSDate?: string;
  srvcEDate?: string;
  srvcConditionLimitTxt?: string;
  srvcImg?: string;
  evntTitle?: string;
  evntSDate?: string;
  evntEDate?: string;
  evntConditionLimitTxt?: string;
  evntTxt?: string;
  popfile?: string;
  chargeNm?: string;
  officetel?: string;
  noticeComment?: string;
  [key: string]: string | undefined; // popfile2, popfile3 등 동적 필드
}

export interface AbandonmentApiResponse {
  response?: {
    header?: {
      resultCode: string;
      resultMsg: string;
    };
    body?: {
      items?: {
        item?: ShelterAnimalItem | ShelterAnimalItem[] | null;
      };
      numOfRows?: number;
      pageNo?: number;
      totalCount?: number;
    };
  };
}
