# live chatting
# import pytchat
#
# live_chat = pytchat.create(video_id="AiqPgy9vnBk")
#
# while live_chat.is_alive():
#     for chat in live_chat.get().sync_items():
#         print(f"{chat.author.name}: {chat.message}")

# from googleapiclient.discovery import build
#
# # API 정보 설정
# api_key = 'api_key'
# youtube = build('youtube', 'v3', developerKey=api_key)
#
# # 비디오 ID 설정 (예: 'v=dQw4w9WgXcQ')
# video_id = 'vid'
#
# # YouTube Data API 호출
# response = youtube.videos().list(
#     part='snippet',
#     id=video_id
# ).execute()
#
# # 응답에서 게임 정보 추출
# for item in response.get('items', []):
#     title = item['snippet']['title']
#     description = item['snippet']['description']
#     tags = item['snippet'].get('tags', [])
#
#     print(f"Title: {title}")
#     print(f"Description: {description}")
#     print(f"Tags: {tags}")
