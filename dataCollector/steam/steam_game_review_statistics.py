import requests
from bs4 import BeautifulSoup

def get_review_summary(app_id):
    url = f"https://store.steampowered.com/app/{app_id}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    recent_reviews = soup.find('span', {'class': 'responsive_reviewdesc_short'})
    all_reviews = soup.find_all('span', {'class': 'responsive_reviewdesc_short'})[1]

    return {
        'all_reviews': recent_reviews.text.strip() if recent_reviews else 'No recent reviews',
        'recent_reviews': all_reviews.text.strip() if all_reviews else 'No reviews'
    }

app_id = 730  # app_id 입력
review_summary = get_review_summary(app_id)

print("Recent reviews:", review_summary['recent_reviews'])
print("All reviews:", review_summary['all_reviews'])