from os import error
import requests
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
# from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException

import pandas as pd
import numpy as np

options = webdriver.ChromeOptions()
options.headless = True
options.add_argument("window-size=1920x1080")
# options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")

browser = webdriver.Chrome()
browser.maximize_window

df_comp_demo = pd.read_csv('preprocessed_df_comp_demo.csv', encoding='cp949', header=0)

# 데이터 분할
div = int(len(df_comp_demo)/10)
df_comp_demo_01 = df_comp_demo[:div]
df_comp_demo_02 = df_comp_demo[div:div*2]
df_comp_demo_03 = df_comp_demo[div*2:div*3]
df_comp_demo_04 = df_comp_demo[div*3:div*4]
df_comp_demo_05 = df_comp_demo[div*4:div*5]
df_comp_demo_06 = df_comp_demo[div*5:div*6]
df_comp_demo_07 = df_comp_demo[div*6:div*7]
df_comp_demo_08 = df_comp_demo[div*7:div*8]
df_comp_demo_09 = df_comp_demo[div*8:div*9]
df_comp_demo_10 = df_comp_demo[div*9:]

#  해당 데이터셋 다시 분할
div = int(len(df_comp_demo_07)/10)
df_comp_demo_07_01 = df_comp_demo_07[:div]
df_comp_demo_07_02 = df_comp_demo_07[div:div*2]
df_comp_demo_07_03 = df_comp_demo_07[div*2:div*3]
df_comp_demo_07_04 = df_comp_demo_07[div*3:div*4]
df_comp_demo_07_05 = df_comp_demo_07[div*4:div*5]
df_comp_demo_02_06 = df_comp_demo_02[div*5:div*6]
df_comp_demo_07_07 = df_comp_demo_07[div*6:div*7]
df_comp_demo_07_08 = df_comp_demo_07[div*7:div*8]
df_comp_demo_07_09 = df_comp_demo_07[div*8:div*9]
df_comp_demo_07_10 = df_comp_demo_07[div*9:]


# print(df_comp_demo.head(5))

names = df_comp_demo_02_06['기업명']   # 02,
# names = ["(주)윤커뮤니케이션즈", "(주)키삭","(주)픽스다인웨이메이커", "(주)소프트넷"]  # 샘플

# print(names)


company_names = []
total_rates = []
salary_rates = []
balance_rates = []
culture_rates = []
promotion_rates = []
executives_rates = []
recommendation_rates = []
CEO_rates = []
growth_rates = []

for name in names:
    url = "https://www.jobplanet.co.kr/welcome/index"
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"}

    browser.get(url)

    elem = browser.find_element_by_class_name("input_search")
    elem.click()
    elem.send_keys(name)
    elem.send_keys(Keys.ENTER)
    time.sleep(2)


    elem02 = browser.find_element_by_class_name("tit")
    try:
        elem02.click()
        time.sleep(2)
    except Exception as e:
        print("등록할 수 없는 기업 : {}".format(name), e)
        continue

    soup = BeautifulSoup(browser.page_source, "lxml")
    # print(soup)

    company_names.append(name)

    total_rate = soup.find("span", attrs={"class":"rate_point"})
    if total_rate:
        total_rate = total_rate.get_text()
        total_rates.append(total_rate)
    else:
        # elem = browser.find_element_by_class_name('btn_close_x_ty1 ')
    
        # elem.click()

        elem03 = browser.find_element_by_class_name("viewReviews")
        elem03.click()
        time.sleep(2)
        soup = BeautifulSoup(browser.page_source, "lxml")
        total_rate = soup.find("span", attrs={"class":"rate_point"})
        total_rate = total_rate.get_text()
        total_rates.append(total_rate)
    # print("total_rates:", total_rates)

    # try:
    #     total_rate = total_rate.get_text()
    #     total_rates.append(total_rate)
    # except Exception as e:
    #     elem03 = browser.find_element_by_class_name("viewReviews")
    #     elem03.click()
    #     total_rate = soup.find("span", attrs={"class":"rate_point"})
    #     total_rate = total_rate.get_text()
    #     total_rates.append(total_rate)
    #     # print("{}의 total_rates 불러오기 실패 :".format(name), e)
    #     # total_rates.append(np.nan)
    #     pass
    # print(total_rates)

    try:
        rates = soup.find_all("span", attrs={"class":"txt_point"})
        # print(rates)

        i = 1
        for rate in rates:
            score = rate.get_text()
            if i == 1:
                salary_rates.append(score)
            elif i == 2:
                balance_rates.append(score)
            elif i == 3:
                culture_rates.append(score)
            elif i == 4:
                promotion_rates.append(score)
            elif i == 5:
                executives_rates.append(score)
            elif i == 6:
                recommendation_rates.append(score)
            elif i == 7:
                CEO_rates.append(score)
            else:
                growth_rates.append(score)


            i+=1
            # print(score)
        # print()
    except Exception as e:
        print("{} 기업의 세분화 점수 부재".format(name))
        salary_rates.append(np.nan)
        balance_rates.append(np.nan)
        culture_rates.append(np.nan)
        promotion_rates.append(np.nan)
        executives_rates.append(np.nan)
        recommendation_rates.append(np.nan)
        CEO_rates.append(np.nan)
        growth_rates.append(np.nan)
        pass

    time.sleep(1)

browser.quit()

# print(company_names)
# print(total_rates)
# print(salary_rates)
# print(balance_rates)
# print(culture_rates)
# print(promotion_rates)
# print(executives_rates)
# print(recommendation_rates)
# print(CEO_rates)
# print(growth_rates)

jobplanet_rates = pd.DataFrame({'기업명': company_names, 'TotalAvg': total_rates, 'Welfare': salary_rates, 'Balance': balance_rates,
                                'Culture': culture_rates, 'Promotion': promotion_rates, 'Executive': executives_rates, 
                                'Recommend':recommendation_rates, 'Support': CEO_rates, 'Growth': growth_rates
                                })


print(jobplanet_rates)
jobplanet_rates.to_csv("jobplanet_rates_02_06.csv", encoding="cp949")
