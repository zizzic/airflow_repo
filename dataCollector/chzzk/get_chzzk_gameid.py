import time
from selenium import webdriver
from bs4 import BeautifulSoup
from game_list import games
import pandas as pd


# get steam game id top 100
game_list = games
# test game_list
# game_list = ['마인크래프트','타르코프','리그오브레전드','펠월드','리썰컴퍼니']

chzzk_game_tags = []
base_url = "https://chzzk.naver.com/search?query="
driver = webdriver.Chrome()


# 자동완성 xpath
# /html/body/div[1]/div/div[3]/div[1]/div/ul/li[1]/a/span[2]

for game in game_list:
    url = base_url + game
    driver.get(url)
    time.sleep(2)
    # 페이지 소스 가져오기 및 BeautifulSoup 객체 생성
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # 'video_card_category__xQ15T' 클래스를 가진 span 태그 찾기
    span_element = soup.find('span', class_='video_card_category__xQ15T')

    # span 요소의 존재 여부 확인
    if span_element:
        game_name = span_element.text
        chzzk_game_tags.append(game_name)
    else:
        pass
driver.quit()

# to CSV
df = pd.DataFrame({"game_id": game_list,
                  "chzzk_game_id": chzzk_game_tags})

df.to_csv("game_list.csv",encoding='utf-8')