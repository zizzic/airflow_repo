from bs4 import BeautifulSoup

# steamdb_charts.html에서 인기 top 1000게임 app_id 가져오기
with open('steamdb_charts.html', 'r', encoding='utf-8') as f:
    html = f.read()

# HTML 파싱
soup = BeautifulSoup(html, 'html.parser')

rows = soup.select("#table-apps > tbody > tr")

for row in rows:
    element = row.select_one("td:nth-child(3) > a")

    # 게임 이름
    game = element.text

    # 게임 app_id
    app_id = element['href'].split('/')[2]

    print(f"Game: {game}, app_id: {app_id}")