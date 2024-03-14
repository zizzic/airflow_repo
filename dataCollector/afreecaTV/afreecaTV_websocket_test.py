import json
import sys
import requests
import websocket
from functools import partial
import ssl


# 웹소켓 메시지 핸들러 정의
def on_message(ws, message):
    try:
        msg_json = json.loads(message)
        print(msg_json)
    except json.JSONDecodeError:
        print("메시지가 JSON 형식이 아닙니다.")


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws):
    print("### Connection closed ###")


def on_open(ws, bj_live_status):
    # 서버에 보낼 메시지를 JSON 형식으로 작성
    message = {
        "SVC": "INIT_GW",
        "RESULT": 0,
        "DATA": {
            "gate_ip": bj_live_status["GWIP"],
            "gate_port": bj_live_status["GWPT"],
            "broadno": bj_live_status["BNO"],
            "category": bj_live_status["CATE"],
            "fanticket": bj_live_status["FTK"],
            "cookie": "",
            "cli_type": 44,
            "cc_cli_type": 19,
            "QUALITY": "normal",
            "guid": "9869C0818D31A268EC13D6711D070CA0",
            "BJID": bj_live_status["BJID"],
            # "addinfo": "ad_lang ko is_auto 0 ",
            "JOINLOG": "log&uuid=cb2f85456a1844db7c6b5b99d618681e&geo_cc=KR&geo_rc=11&acpt_lang=ko_KR&svc_lang=ko_KR&os=mac&is_streamer=false&is_rejoin=false&is_auto=false&is_support_adaptive=false&uuid_3rd=cb2f85456a1844db7c6b5b99d618681e&subscribe=-1liveualog&is_clearmode=false&lowlatency=0",
            "update_info": 0,
        },
    }

    # print(message)
    ws.send(json.dumps(message))
    print("Message sent to the server")


if __name__ == "__main__":
    # bj_id 필요
    bj_id = ["dudadi770"]
    live_status = {"bid": bj_id, "type": "live"}
    # channel_id에 맞춰서 websocet chat domain을 구함
    bj_live_status = requests.post(
        f"https://live.afreecatv.com/afreeca/player_live_api.php?bjid={bj_id[0]}",
        live_status,
    ).json()["CHANNEL"]
    print(bj_live_status)
    # 'RESULT' is 1 when bj is live, and 0 when bj is off.
    if not bj_live_status["RESULT"]:
        print("RESULT is 0. BJ is not on live.")
        sys.exit(0)
    else:
        chat_domain = bj_live_status["CHDOMAIN"]

    chat_wss_url = f"wss://{chat_domain}:8001/Websocket/{bj_id[0]}"
    livestat_wss_url = f"wss://bridge.afreecatv.com/Websocket/{bj_id[0]}"
    print(livestat_wss_url)
    websocket.enableTrace(True)

    # live status 가져오기
    sslopt = {"cert_reqs": ssl.CERT_NONE}  # 인증서 검증 관련 test code
    ws = websocket.WebSocketApp(
        "wss://bridge.afreecatv.com/Websocket",
        on_open=partial(on_open, bj_live_status=bj_live_status),
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(sslopt=sslopt)
