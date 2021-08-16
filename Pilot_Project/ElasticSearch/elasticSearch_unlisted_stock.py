# get_data(data_type ): # company_name

from elasticsearch import Elasticsearch
import datetime
import pandas as pd
import numpy as np

full_cols = []

# 네트워크 설정
es = Elasticsearch(
    host="112.175.39.166",
    port=9200,
    http_auth=('id', 'pw'),
    timeout=500,   # 조회시간 설정
    max_retries=3,
    retry_on_timeout=True
)

# 질의문
query = {
    "size": 2000,
    "sort": {
        "SearchDate": "desc"
    },
    "query": {
        "bool": {
            "must": [
                {"match": {"DataType": "unlisted_stock"}}, # 조건 주기
            ],
            "filter": {
                "range": {
                    "SearchDate": {
                        "gte": "2021-05-01", # 수집된 날짜 설정, 시작일
                        "lte": datetime.datetime.now().strftime('%Y-%m-%d') # 마지막일
                    }
                }
            }
        }
    }
}

# 질의문에서 데이터가 저장된 source_data 가져오기
result = es.search(index="source_data", body=query)

# source_data 내용 확인
# print("result:", result)

#데이터가 있는 경우 data_list에 담기
data_list = []
if result["hits"]["total"] > 0:
    for data in result["hits"]["hits"]:
        data_list.append(data["_source"])

else:
    pass
# print("data_list:", data_list)

# 수집된 데이터 row 확인
# print('length of list', len(data_list))

# 조회된 데이터에서 필요한 컬럼 선택하기
#temp.append(data['column']),   column에 추출하기 원하는 컬럼값을 넣어주면 됨. 변수정의서 참조

try:
    if data_list:
        for data in data_list:
            if data["Data"]:
                tmp = []
                tmp.append(data["CompanyName"]) # 기업명
                tmp.append(data["BusinessNum"]) # 사업자번호
                tmp.append(data["CompanyCode"]) # 기업코드
                # tmp.append(data["CeoName"])
                tmp.append(data["SearchDate"])  # 데이터 수집 날짜
                tmp.append(data["Data"]['CompanyValue']) # 기업가치
                tmp.append(data["Data"]['StandardPrice'])  # 기준가
                tmp.append(data["Data"]['PriceChange'])  # 가격변동(%)
                tmp.append(data["Data"]['TransPrice'])  # 거래대금(1주일)
                tmp.append(data["Data"]['StockCode']) # 종목코드
                # tmp.append(data["Data"]['TransData']['TransForm']) # 경영진 평점
                # tmp.append(data["Data"]['TransData']['TransNum'])  # 순번
                # tmp.append(data["Data"]['TransData']['TransPrice'])  # 직종
                # tmp.append(data["Data"]['TransData']['TransAmount']) # 현직원 여부
                # tmp.append(data["Data"]['TransData']['MinAmount'])  # 리뷰날짜
                # tmp.append(data["Data"]['TransData']['TransDate'])   # 총점
                tmp.append(data["Data"]['FinanceData']['Year']) # 기간
                tmp.append(data["Data"]['FinanceData']['Revenue'])   # 매출액
                tmp.append(data["Data"]['FinanceData']['OperProfit'])   # 영업이익(손실)
                tmp.append(data["Data"]['FinanceData']['NetIncome'])   # 당기순이익(손실)
                tmp.append(data["Data"]['FinanceData']['TotalAsset']) # 자산총계
                tmp.append(data["Data"]['FinanceData']['TotalLiability'])    # 부채총계
                tmp.append(data["Data"]['FinanceData']['TotalStock'])  # 자본총계
                tmp.append(data["Data"]['FinanceData']['OperProfitRate'])   # 영업이익률
                tmp.append(data["Data"]['FinanceData']['StockRate'])   # 부채비율
                tmp.append(data["Data"]['FinanceData']['ROE'])   # ROE
                full_cols.append(tmp)
except Exception as e:
    print(e)

# # 선택한 컬럼별 데이터 확인
# print("full_cols:", full_cols)

# # 데이터프레임에 담기
df = pd.DataFrame(
        np.array(full_cols, dtype=object),
        # 컬럼명 설정
        columns=["기업명", "사업자번호", "기업코드", "데이터 수집 날짜", "기업가치", "기준가", "가격변동(%)", "거래대금(1주일)", "종목코드", "기간", "매출액",
                 "영업이익(손실)", "당기순이익(손실)", "자산총계", "부채총계", "자본총계", "영업이익률", "부채비율", "ROE"]
    )
# print("df:", df)

# csv로 export
df.to_csv("비상장.csv", encoding='utf-8-sig')
