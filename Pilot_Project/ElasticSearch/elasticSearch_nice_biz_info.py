# get_data(data_type ): # company_name

from elasticsearch import Elasticsearch
import datetime
import pandas as pd
import numpy as np

full_cols = []


es = Elasticsearch(
    host="112.175.39.166",
    port=9200,
    http_auth=('id', 'pw'),
    timeout=500,
    max_retries=3,
    retry_on_timeout=True
)
query = {
    "size": 1000,
    "sort": {
        "SearchDate": "desc"
    },
    "query": {
        "bool": {
            "must": [
                {"match": {"DataType": "nice_biz_info"}},
                # {"match": {"CompanyName": re_type(company_name)}}
            ],
            "filter": {
                "range": {
                    "SearchDate": {
                        "gte": "2015-05-01",
                        "lte": datetime.datetime.now().strftime('%Y-%m-%d')
                    }
                }
            }
        }
    }
}
result = es.search(index="source_data", body=query)
print("result:", result)

data_list = []
if result["hits"]["total"] > 0:
    for data in result["hits"]["hits"]:
        data_list.append(data["_source"])
        # print("data_list:", data_list)
else:
    pass
# print('length of list', len(data_list))


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
                # tmp.append(data["Data"]['Ceo']) # 기업 전체 평정
                tmp.append(data["Data"]['CompType'])  # 복지 및 급여에 대한 평점
                tmp.append(data["Data"]['Industry'])  # 업무와 삶의 균형 평점
                tmp.append(data["Data"]['Owner'])  # 기업문화 평점
                tmp.append(data["Data"]['GroupType']) # 승진 기회 및 가능성 평점
                tmp.append(data["Data"]['MainArea'])  # 순번
                tmp.append(data["Data"]['AvgWage'])  # 직종
                tmp.append(data["Data"]['NewerAvgWage']) # 현직원 여부
                tmp.append(data["Data"]['NumEmp'])
                tmp.append(data["Data"]['JoinRate'])
                tmp.append(data["Data"]['JoinNum'])
                tmp.append(data["Data"]['ResignRate'])
                tmp.append(data["Data"]['ResignNum'])
                tmp.append(data["Data"]['NumYears'])
                full_cols.append(tmp)
except Exception as e:
    print(e)

# print("full_cols:", full_cols)


df = pd.DataFrame(
        np.array(full_cols),
        columns=["기업명", "사업자번호", "기업코드", "데이터 수집 날짜", '기업형태', '산업', '회장성명',
                 '그룹구분', '주력업종', '예상 평균연봉', '올해 입사자 평균연봉', '종업원수', '입사율', '연간 입사자',
                 '퇴사율', '연간 퇴사자', '업력'] # 18
    )

print("df:", df)

df.to_csv("nice_biz_info.csv", encoding='utf-8-sig')
