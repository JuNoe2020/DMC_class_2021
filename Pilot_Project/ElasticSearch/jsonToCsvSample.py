import pandas
import pandas as pd
from elasticsearch import Elasticsearch
import numpy as np

Koita = []

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

def get_data(data_type, company_name):
    try:
        es = Elasticsearch(
            host="112.175.39.166",
            port=9200,
            http_auth=('id', 'pw'),
            timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        query = {
            "size": 100,
            "sort": {
                "SearchDate": "desc"
            },
            "query": {
                "bool": {
                    "must": [
                        {"match": {"DataType": data_type}},
                        {"match": {"CompanyName": re_type(company_name)}}
                    ],
                    "filter": {
                        "range": {
                            "SearchDate": {
                                "gte": "2021-08-01"
                            }
                        }
                    }
                }
            }
        }
        result = es.search(index="source_data", body=query)
        data_list = []
        if result["hits"]["total"] > 0:
            for data in result["hits"]["hits"]:
                data_list.append(data["_source"])
            return data_list
        else:
            return None
    except Exception as e:
        print(e)
        return None

def koita_df(company_name):
    try:
        data_list = get_data("koita", re_type(company_name))
        if data_list:
            for data in data_list:
                if data["Data"]:
                    tmp = []
                    tmp.append(data["CompanyName"])
                    tmp.append(data["BusinessNum"])
                    tmp.append(data["CompanyCode"])
                    tmp.append(data["CeoName"])
                    tmp.append(data["Data"]["CompName"])
                    tmp.append(data["Data"]["LabName"])
                    tmp.append(data["Data"]["CompSize"])
                    tmp.append(data["Data"]["StudyField"])
                    tmp.append(data["Data"]["LabType"])
                    Koita.append(tmp)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    company_name_list = [
        # 미존재 데이터
        "(주)위세아이텍", "콘텔라(주)", "(주)다비오", "(주)이노피아테크", "유엔젤",
        # 존재하는 데이터
        "(주)서일시스템", "(주)나우테스테크놀러지"
    ]

    for company_name in company_name_list:
        koita_df(company_name)

    if Koita:
        df = pd.DataFrame(
            np.array(Koita),
            columns=["기업명", "사업자번호", "기업코드", "대표자", "기업명", "연구소/전담부서명", "규모", "연구분야", "구분"]
        )
        df.to_csv("koita.csv", encoding='utf-8-sig')
    # koita의 경우 데이터가 대부분 None
    else:
        print("noData")
