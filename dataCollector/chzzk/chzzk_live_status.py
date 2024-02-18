import requests
import pandas as pd
import json
from datetime import datetime

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()

# df_structure
df = pd.DataFrame(columns=["STREAMER_ID", "LIVE_COLLECT_TIME_STAMP","VIDEO_TITLE", "GAME_CODE", "VIEWER_NUM"])
answer = []

# get streamer_id in redis
streamer_id = ['f39c3d74e33a81ab3080356b91bb8de5','bee4b42475b5937226b8b7ccbe2eb2dc','4d4a3e04937947aae221fc458cb902f7']

for id in streamer_id:
    # get response
    res = requests.get(f"https://api.chzzk.naver.com/polling/v2/channels/{id}/live-status")
    live_data = res.json()

    # check live_status
    live=live_data["content"]["status"]
    timestamp = datetime.now().isoformat()  # context("data_interval_end")

    if live == "OPEN":
        # status["STREAMER_ID","LIVE_COLLECT_TIME_STAMP","VIDEO_TITLE","GAME_CODE","VIEWER_NUM"]
        video_title = live_data["content"]["liveTitle"]
        game_code = live_data["content"]["liveCategoryValue"] #KR name
        viewer_num = int(live_data["content"]["concurrentUserCount"])

        status = [id,timestamp,video_title,game_code,viewer_num]
        df.loc[len(df)] = status
        answer.append(status)

    else:
        pass

# print(answer)
json_string = json.dumps(answer,ensure_ascii=False,indent=4)
print(json_string)


