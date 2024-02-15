from selenium import webdriver
from bs4 import BeautifulSoup
import time

name_list = ["한동숙입니다", "울프Wolf", "녹두로로", "이춘향이오", "풍월량월풍월량", "찐짜탬탬버린", "랄로10", "삼식123", "괴물쥐", "핫다주", "김뚜띠2", "따효니DDaHyoNi", "강퀴88", "파카", "옥냥이RoofTopCat", "김도KimDoe", "지누", "피닉스박", "죠니월드", "플러리", "러너Runner", "아야츠노유니본인", "아이리칸나에요"]

base_url = "https://chzzk.naver.com/search?query="

# Selenium 웹드라이버 실행
driver = webdriver.Chrome()

for name in name_list:
    url = base_url + name

    # 웹 페이지 열기
    driver.get(url)
    time.sleep(2)
    # 웹 페이지 소스 가져오기
    html = driver.page_source

    # BeautifulSoup으로 파싱
    soup = BeautifulSoup(html, 'html.parser')

    # XPath에 해당하는 요소 찾기
    element = soup.select_one('html > body > div:nth-of-type(1) > div > div:nth-of-type(4) > div > section > section:nth-of-type(1) > div:nth-of-type(1) > div:nth-of-type(1) > a:nth-of-type(1)')

    if element:
        id_value = element['href']
        print(name, id_value)
    else:
        print(name, "ID를 찾을 수 없습니다.")

# Selenium 웹드라이버 종료
driver.quit()
