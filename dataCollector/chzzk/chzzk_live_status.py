import requests
import json

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


channel_id = ['153f28bc51764c3b03d34ea06492f727']

# chzzk_url = 'https://chzzk.naver.com/live/'
# IF LIVE STREAM IS TRUE
# CHECK LIVE STREAM - plz code input iter-code
# res=requests.get(f'https://api.chzzk.naver.com/service/v1/channels/{streamer_uid[0]}')
# print(check_live)
res = requests.get(f"https://api.chzzk.naver.com/polling/v2/channels/{channel_id[0]}/live-status")
check_live = res.json()
print(check_live)
print(json.dumps(check_live,indent=4,ensure_ascii=False))

# Crawling functions that operate asynchronously
# streaming data
# - viewers' participation: current_view_count, accumulated_views,
# - broadcast time and cycle: broadcast_time
# - viewer's change: broadcast_title, timestamp
# - game info: game_info


# async def chzzk_main():
#     driver = webdriver.Chrome()
#     driver.get("http://www.python.org")
#     assert "Python" in driver.title
#     elem = driver.find_element(By.NAME, "q")
#     elem.clear()
#     elem.send_keys("pycon")
#     elem.send_keys(Keys.RETURN)
#     assert "No results found." not in driver.page_source
#     driver.close()