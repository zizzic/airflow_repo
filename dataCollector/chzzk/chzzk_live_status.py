import requests
import pandas as pd
import json
from datetime import datetime

# df_structure
df = pd.DataFrame(columns=["STREAMER_ID", "LIVE_COLLECT_TIME_STAMP","VIDEO_TITLE", "GAME_CODE", "VIEWER_NUM"])

# get streamer_id in redis
streamer_id = ['0b33823ac81de48d5b78a38cdbc0ab94']

for id in streamer_id:
    # get response
    res = requests.get(f"https://api.chzzk.naver.com/polling/v2/channels/{id}/live-status")
    live_data = res.json()

    # check live_status
    live=live_data["content"]["status"]
    timestamp = datetime.now()  # context("data_interval_end")

    if live == "OPEN":
        # status["STREAMER_ID","LIVE_COLLECT_TIME_STAMP","VIDEO_TITLE","GAME_CODE","VIEWER_NUM"]
        video_title = live_data["content"]["liveTitle"]
        game_code = live_data["content"]["liveCategoryValue"] #KR name
        viewer_num = int(live_data["content"]["concurrentUserCount"])

        status = [id,timestamp,video_title,game_code,viewer_num]
        df.loc[len(df)] = status

    else:
        pass


