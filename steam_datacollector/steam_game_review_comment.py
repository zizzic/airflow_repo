import requests

def get_game_reviews(app_id):
    # API URL 설정
    api_url = f'https://store.steampowered.com/appreviews/{app_id}?json=1'

    # API 키를 설정합니다. (https://steamcommunity.com/dev/apikey 에서 키를 발급받을 수 있습니다.)
    api_key = '9B38F89D2AFA8356EACB2BBE13CB20D1'

    # API 요청 매개변수 설정
    params = {
        'key': api_key,
        'json': '1',
        'language': 'korean',  # 한국어 리뷰만 가져오기
        'num_per_page': 100,  # 페이지당 리뷰 수
    }

    while True:
        # API 요청 보내기
        response = requests.get(api_url, params=params)

        # 응답 확인
        if response.status_code == 200:
            data = response.json()
            reviews = data.get('reviews', [])
            for review in reviews:
                print(f"Review ID: {review['recommendationid']}")
                print(f"Author: {review['author']['steamid']}")
                print(f"Review: {review['review']}")
                print(f"Recommended: {review['voted_up']}")
                print(f"----------------------------------")

            # 리뷰가 100개 이상인 경우 다음 페이지로 이동 (페이지네이션)
            if data['cursor'] != '*':
                params['cursor'] = data['cursor']
            else:
                break
        else:
            print(f"Error: Unable to fetch reviews. Status Code: {response.status_code}")
            break

# 게임의 app_id (예: Palworld)
app_id = 1623730

# Steam Community API로부터 리뷰 가져오기
get_game_reviews(app_id)