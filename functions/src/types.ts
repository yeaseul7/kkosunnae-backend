export interface ShelterAnimalItem {
  desertionNo?: string;
  filename?: string;
  happenDt?: string;
  happenPlace?: string;
  kindCd?: string;
  colorCd?: string;
  age?: string;
  weight?: string;
  noticeNo?: string;
  noticeSdt?: string;
  noticeEdt?: string;
  popfile?: string;
  processState?: string;
  sexCd?: string;
  neuterYn?: string;
  specialMark?: string;
  careRegNo?: string; // 보호소번호
  careNm?: string;
  careTel?: string;
  careAddr?: string;
  orgNm?: string;
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
