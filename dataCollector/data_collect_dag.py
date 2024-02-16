from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import time
from datetime import datetime

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs


# DAG 정의
@dag(schedule_interval='@daily', start_date=days_ago(1), catchup=False)
def get_data():

    # get streamer_list in elasticcache
    @task
    def get_streamer_list():
        # test streamer_list
        return ["0d027498b18371674fac3ed17247e6b8","fe558c6d1b8ef3206ac0bc0419f3f564","0b33823ac81de48d5b78a38cdbc0ab94"]


    # chzzk_method
    @task
    def get_chzzk_data(s_id_list: list,**kwargs):
        # df_structure
        df = pd.DataFrame(columns=["STREAMER_ID","BROADCAST_ID", "LIVE_COLLECT_TIME", "CHZ_BROADCAST_TITLE","AFC_BROADCAST_TITLE","CHZ_GAME_CODE","AFC_GAME_CODE", "CHZ_VIEWER_NUM","AFC_VIEWER_NUM"])
        broadcast_id = ""
        afc_broadcast_title=""
        afc_game_code=""
        afc_viewer_num=0

        for streamer_id in s_id_list:
            # get response
            res = requests.get(f"https://api.chzzk.naver.com/polling/v2/channels/{id}/live-status")
            live_data = res.json()

            # check live_status
            live = live_data["content"]["status"]
            timestamp = kwargs("data_interval_end").strftime("%Y-%m-%d %H:%M")

            if live == "OPEN":
                # status["STREAMER_ID","LIVE_COLLECT_TIME_STAMP","VIDEO_TITLE","GAME_CODE","VIEWER_NUM"]
                video_title = live_data["content"]["liveTitle"]
                game_code = live_data["content"]["liveCategoryValue"]  # KR name
                viewer_num = int(live_data["content"]["concurrentUserCount"])

                status = [streamer_id, broadcast_id, timestamp, video_title, afc_broadcast_title, game_code, afc_game_code, viewer_num,afc_viewer_num]
                df.loc[len(df)] = status

            else:
                pass
        return df

    @task
    def afreeca_data(bj_ids: list, **kwargs):
        def get_live_status(bjid, headers):
            # BJ의 생방송 상태와 game category, broadcast title 등의 정보를 조회하는 함수
            live_status_url = (
                f"https://live.afreecatv.com/afreeca/player_live_api.php?bjid={bjid}"
            )
            live_res = requests.post(
                live_status_url, headers=headers, data={"bid": bjid, "type": "live"}
            ).json()["CHANNEL"]
            return live_res

        def get_broad_info(bjid, headers):
            # BJ의 생방송 상태, 실시간 시청자 수, timestamp를 조회하는 함수
            broad_url = f"https://bjapi.afreecatv.com/api/{bjid}/station"
            broad_res = requests.get(broad_url, headers=headers).json()
            return broad_res


        # 연결을 위한 헤더와 parameter설정
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        }

        # live stream data를 저장할 리스트 선언
        live_stream_data = []

        start_time = time.time()

        for bjid in bj_ids:
            live_res = get_live_status(bjid, headers)
            broad_res = get_broad_info(bjid, headers)
            broad_info = broad_res.get("broad")

            try:
                # if live is on, RESULT : 1 and res_broad_info is not null
                if live_res.get("RESULT") and broad_info:
                    live_stream_data.append(
                        {
                            "STREAMER_ID": bjid,
                            "BROADCAST_ID": live_res["BNO"],
                            "LIVE_COLLECT_TIME": broad_res["current_timestamp"].strftime("%Y-%m-%d %H:%M"),
                            "CHZ_BROADCAST_TITLE": "",
                            "AFC_BROADCAST_TITLE": live_res["TITLE"],
                            "CHZ_GAME_CODE": "",
                            "AFC_GAME_CODE": int(live_res["CATE"]),
                            "CHZ_VIEWER_NUM": 0,
                            "AFC_VIEWER_NUM": broad_info["current_sum_viewer"],
                        }
                    )
                # else:
                # print(f"bj {bjid} is not on live.")
            except ValueError:
                pass

        # dataframe에 저장
        live_stream_df = pd.DataFrame(live_stream_data)
        return live_stream_df


    @task
    def merge_stream_data(chzzk_data: pd.DataFrame,afreeca_data: pd.DataFrame) -> pd.DataFrame:
        stream_data = pd.concat([chzzk_data,afreeca_data], ignore_index=True)
        return stream_data

    @task
    def load(data):
        # 데이터 로드 로직
        print(f"Loaded data: {transformed_data['transformed_data']}")

    # 태스크 실행 순서 지정
    streamer_list = get_streamer_list()
    chzzk_data = get_chzzk_data(streamer_list)
    afreeca_data = get_afreeca_data(streamer_list)
    stream_data = merge_stream_data(chzzk_data,afreeca_data)

    load(stream_data)

# DAG 인스턴스 생성
example_dag = get_data()
