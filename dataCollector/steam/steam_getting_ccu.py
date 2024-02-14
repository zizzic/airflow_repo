import pandas as pd
import requests

# CSV 파일 다운 받기
url = "https://raw.githubusercontent.com/dgibbs64/SteamCMD-AppID-List/main/steamcmd_appid.csv"
response = requests.get(url)
with open('steamcmd_appid.csv', 'w', encoding='utf-8') as f:
    f.write(response.text)

# CSV파일 DF로 바꾸기
df = pd.read_csv('steamcmd_appid.csv')

# 컬럼명 바꾸기
df.columns = ['app_id', 'name']

# DF의 Values로 API를 호출해 게임별 CCU를 가져오기
cnt = 1
for index, row in df.iterrows():
    app_id, name = row['app_id'], row['name']
    
    url = f"http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1?appid={app_id}"
    response = requests.get(url)
    data = response.json()
    
    # result의 값이 1인 경우에만 CCU를 제공
    if data['response']['result'] != 1:
        continue
    
    ccu = data['response']['player_count']
    print(app_id, name)
    print(f'CCU of {name}: {ccu}')
    
    # test이므로 16만여개 중 50개만 가져오기
    cnt += 1
    if cnt >= 50:
        break