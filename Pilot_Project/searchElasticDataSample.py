import datetime
from elasticsearch import Elasticsearch


# (주), (유), (사) 등 제거
def re_type(company_name):
    if company_name and company_name != "":
        start = company_name.find("(")
        end = company_name.find(")")
        if start > -1 and end > -1:
            return company_name.replace(company_name[start:end+1], "")
        else:
            return company_name
    else:
        return ""


def search_elastic_data():
    try:
        es = Elasticsearch(
            host="112.175.39.166",
            port=9200,
            http_auth=(ID, PASS),
            timeout=60,
            max_retries=3,
            retry_on_timeout=True
        )

        # 데이터타입(DataType) 종류
        # NTIS_과제목록(ntis_assignments), NTIS_성과목록(ntis_accomplishments)
        # KIPRIS_국내(kipris_domestic), KIPRIS_패밀리(kipris_family)
        # KISTI_논문(kisti_article), KISTI_특허(kisti_patent), KISTI_보고서(kisti_report)
        # 온라인뉴스(naver_news), 블로그(naverBlog), 카페(naverCafe), 쇼핑_다나와(danawa), 네이버_트렌드(naverTrend)
        # 잡플래닛_통계리뷰(jobplanet_review), 잡플래닛_프리미엄리뷰(jobplanet_premium), 사람인(saramin), 팀블라인드(teamblind)
        # NTIS_웹(ntis_web), KOITA(koita), NICE_BIZ_INFO(nice_biz_info), KCI(kci)
        # 네이버_파이낸스(naver_finance), 비상장(unlisted_stock), DART(dart), ISTANS(istans)

        data_type = "naver_news"
        query = {
            "size": 1,                      # 한번에 최대 10000개 문서 가져올 수 있음 (10000개가 넘는다면 조건을 줘서 쪼개서 가져오기 ex)날짜 조건)
            "sort": {"SearchDate": "desc"},     # 최근 날짜부터
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "DataType": data_type
                        }
                    },
                    "filter": {
                        "range": {
                            "SearchDate": {
                                "gte": "2021-08-01",  # 8월 1일 데이터부터 가져오시는 것을 추천드립니다.
                                "lte": "now"
                            }
                        }
                    }
                }
            }
        }

        # 샘플 쿼리
        # data_type = "naver_news"
        # company_name = "(주)이노그리드"
        # 기업명으로 검색을 할 경우, '(주)','(유)','(사)' 등의 단어 제거 후 검색 요망
        # company_name_ = re_type(company_name)  # re_type 함수 참고
        # sample_query = {
        #     {
        #         "_source": [],
        #         "size": 10000,
        #         "query": {
        #             "bool": {
        #                 # 다중조건 검색 예시 (데이터타입 + 기업명)
        #                 "must": [
        #                     {"match": {"DataType": data_type}},
        #                     {"match": {"CompanyName": company_name_}}
        #                 ],
        #                 "filter": {
        #                     "range": {
        #                         "SearchDate": {
        #                             "gte": "2021-08-01",
        #                             "lte": datetime.datetime.now().strftime('%Y-%m-%d')   # 오늘 날짜 입력 예시
        #                         }
        #                     }
        #                 }
        #             }
        #         }
        #     }
        # }

        # 쿼리 실행
        res = es.search(index="source_data", body=query)
        # print(res, end="\n\n")

        # 데이터 총 개수
        num_data = int(res['hits']['total'])
        print('데이터 총 개수: {}'.format(num_data), end="\n\n")

        # 데이터 파싱
        for idx in range(len(res['hits']['hits'])):
            data = res['hits']['hits'][idx]['_source']
            print(data, end="\n\n")

        return "success", res

    except Exception as error:
        print(error)
        return "Error"


if __name__ == "__main__":
    flag, data = search_elastic_data()
    # if flag == "success":
    #     print(data)
