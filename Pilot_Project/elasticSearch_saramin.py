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
    http_auth=(ID, Pass),
    timeout=500,   # 조회시간 설정
    max_retries=3,
    retry_on_timeout=True
)

# 질의문
query = {
    "size": 10,
    "sort": {
        "SearchDate": "desc"
    },
    "query": {
        "bool": {
            "must": [
                {"match": {"DataType": "saramin"}}, # 조건 주기
            ],
            "filter": {
                "range": {
                    "SearchDate": {
                        "gte": "2017-05-01", # 수집된 날짜 설정, 시작일
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
print("result:", result)

# 데이터가 있는 경우 data_list에 담기
data_list = []
if result["hits"]["total"] > 0:
    for data in result["hits"]["hits"]:
        data_list.append(data["_source"])
        # print("data_list:", data_list)
else:
    pass

# 수집된 데이터 row 확인
print('length of list', len(data_list))

# 조회된 데이터에서 필요한 컬럼 선택하기
# temp.append(data['column']), column에 추출하기 원하는 컬럼값을 넣어주면 됨. 변수정의서 참조
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
                tmp.append(data["Data"]['Industry']) # 기업 전체 평정
                tmp.append(data["Data"]['NumEmp'])  # 복지 및 급여에 대한 평점
                tmp.append(data["Data"]['NumYears'])  # 업무와 삶의 균형 평점
                tmp.append(data["Data"]['CompSize'])  # 기업문화 평점

                # for i in range(len(data["Data"]["Review"][0])):
                #     tmp.append(data["Data"]['Review'][0][i]['Question'])  # 순번
                #     tmp.append(data["Data"]['Review'][0][i]['NumAns'])  # 직종
                #     tmp.append(data["Data"]['Review'][0][i]['Answer']) # 현직원 여부

                full_cols.append(tmp)
except Exception as e:
    print(e)

# 선택한 컬럼별 데이터 확인
print("full_cols:", full_cols)
#
# # 데이터프레임에 담기
#
df = pd.DataFrame(
        np.array(full_cols),
        # 컬럼명 설정
        columns=["기업명", "사업자번호", "기업코드", "데이터 수집 날짜", "직종", "기업 규모", "직원수", "업력"]
    )

print("df:", df)
#
# # # csv로 export
# df.to_csv("saramin.csv", encoding='utf-8-sig')
# #
#
