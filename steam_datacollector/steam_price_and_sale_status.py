import requests

# 게임의 appid
appid = '1623730' # PalWorld

# API requests
response = requests.get(f'http://store.steampowered.com/api/appdetails?appids={appid}&cc=kr')

# JSON 파싱
data = response.json()

# 가격 얻기
price = data[appid]['data']['price_overview']['final_formatted']

# 할인율 체크
is_on_sale = data[appid]['data']['price_overview']['discount_percent'] > 0


print(f"Price: {price}")
if is_on_sale:
    print("The game is on sale.")
else:
    print("The game is not on sale.")