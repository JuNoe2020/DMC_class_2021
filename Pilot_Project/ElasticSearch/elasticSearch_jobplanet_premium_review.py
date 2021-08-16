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
    "size": 1900,
    "sort": {
        "SearchDate": "desc"
    },
    "query": {
        "bool": {
            "must": [
                {"match": {"DataType": "jobplanet_premium"}},
                # {"match": {"CompanyName": re_type(company_name)}}
            ],
            "filter": {
                "range": {
                    "SearchDate": {
                        "gte": "2021-05-01",
                        "lte": datetime.datetime.now().strftime('%Y-%m-%d')
                    }
                }
            }
        }
    }
}
result = es.search(index="source_data", body=query)
# print("result:", result)

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
            if data['Data']:
                # print("Data: ", data)
                tmp = []
                tmp.append(data["CompanyName"]) # 기업명
                # print(data['CompanyName'])
                tmp.append(data["BusinessNum"]) # 사업자번호
                tmp.append(data["CompanyCode"]) # 기업코드
                # tmp.append(data["CeoName"])
                # tmp.append(data["SearchDate"])  # 데이터 수집 날짜

                for i in range(10, 30):
                    # tmp.append(data["Data"]['PremiumReview'][i]['Seq'])  # 순번
                    # tmp.append(data["Data"]['PremiumReview'][i]['Category'])  # 카테고리
                    # tmp.append(data["Data"]['PremiumReview'][i]['PreReviewType']) # 현직원 여부
                    # tmp.append(data["Data"]['PremiumReview'][i]['Question'])  # 리뷰날짜
                    answers = data["Data"]['PremiumReview'][i]['Answer']
                    # print("answers :", answers)

#                     # 선택형 질문
                    try:
                        if int(answers[1]) < 101:
                            num = 1
                            select_answer = []
                            while num <= len(answers)/2:
                                select_answer.append(int(answers[(num*2)-1]))
                                num += 1
                            max_value = max(select_answer)
                            # print("max:", max_value)
                            str_max_value = str(max_value)
                            # print("str_max_value :", str_max_value)

                            answer_idx = answers.index(str_max_value)
                            true_answer_idx = int(answer_idx) - 1
                            # print("true_answer_idx:", true_answer_idx)
                            # print(answers[true_answer_idx][3:])
                            answers = answers[true_answer_idx][3:]
                            # tmp.append(true_answer)
                            # answers == max_value
                    except:
                        answers = " ".join(answers)
                        # print("joined_answers :", answers)

                    tmp.append(answers)
#                     # print("tmp: ", tmp)
                    i+=1
                full_cols.append(tmp)
                # print("full_cols :", full_cols)
except Exception as e:
    print(e)
# #
print("full_cols:", full_cols)
# print("length of full_cols :", len(full_cols[0]))
# # print("shape of full_cols :", full_cols.shape)
# #
# #
df = pd.DataFrame(
        np.array(full_cols),
        columns=["기업명", "사업자번호", "기업코드", 'Q.\n당신의 상사는 어떤 유형인가요?',  'Q.\n당신의 동료는 어떤 유형인가요?', 'Q.\n경영진은 어떤 유형인가요?',
                 'Q.\n업무 강도와 일정은 어떠한가요?', 'Q.\n협업은 어떤 방식으로 이루어지나요?', 'Q.\n의사결정은 어떻게 이루어지나요?', 'Q.\n저연차의 실무자도 주요 사항을 함께 논의할 수 있나요?',
                 'Q.\n업무에 필요한 충분한 도구 및 자원을 제공 받고 있나요?', 'Q.\n부서 혹은 팀은 어떤 방식으로 업무를 진행하나요?', 'Q.\n일을 더 잘하기 위해 어떤 부분이 개선되면 좋을까요?',
                 'Q.\n기업 문화가 점점 좋아지는 것을 체감하고 있나요?', 'Q.\n회사의 비전이나 목표에 공감하고 있나요?', 'Q.\n업무를 하다 자유롭게 쉴 수 있나요?',
                 'Q.\n최근 3개월 동안 휴식시간에 대화를 나눈 가장 높은 직급은요?', 'Q.\n복지 제도, 워크샵, 행사 등은 어떻게 기획하고 진행하나요?', 'Q.\n회사에 건의사항을 편하게 얘기할 수 있나요?',
                 'Q.\n사내 정치가 존재하나요?', 'Q.\n사내 분위기는 대체적으로 어떤가요?', 'Q.\n우리 회사의 업무환경(위치, 사무공간, 휴게공간 등)은 어때요?', 'Q.\n우리 회사에 잘 적응할 수 있는 사람은 어떤 사람인가요?'
                 ])
print("df:", df)
df.to_csv("jobplanet_premium.csv", encoding='utf-8-sig')
