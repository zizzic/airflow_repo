import json
import sys
import requests
import websocket
from functools import partial


# 웹소켓 메시지 핸들러 정의
def on_message(ws, message):
    try:
        # 메시지 문자열에서 JSON 객체로 파싱
        msg_json = json.loads(message)
        # 'bdy' 키가 있는지 확인하고, 리스트의 마지막 요소에서 'msg' 필드 추출
        if "bdy" in msg_json and len(msg_json["bdy"]) > 0:
            # 마지막 메시지 객체
            last_msg_obj = msg_json["bdy"][-1]
            # 'msg' 필드 값 추출
            if "msg" in last_msg_obj:
                last_msg = last_msg_obj["msg"]
                print("가장 최근 메시지:", last_msg)
            else:
                print("마지막 메시지 객체에 'msg' 필드가 없습니다.")
        else:
            print("'bdy' 필드가 없거나 비어 있습니다.")
    except json.JSONDecodeError:
        print("메시지가 JSON 형식이 아닙니다.")


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws):
    print("### Connection closed ###")


def on_open(ws, cid, accTkn):
    # 서버에 보낼 메시지를 JSON 형식으로 작성
    message = {
        "ver": "2",
        "cmd": 100,
        "svcid": "game",
        "cid": cid,
        "tid": 1,
        "bdy": {"uid": None, "devType": 2001, "accTkn": accTkn, "auth": "READ"},
    }
    # JSON 객체를 문자열로 변환하여 서버에 보냄
    ws.send(json.dumps(message))
    print("Message sent to the server")


if __name__ == "__main__":
    # channel_id 필요
    channel_id = ["1a1dd9ce56fb61a37ffb6f69f6d5b978"]
    # channel_id에 맞춰서 chat_channel_id를 구하고
    chat_channel_content = requests.get(
        f"https://api.chzzk.naver.com/polling/v2/channels/{channel_id[0]}/live-status"
    ).json()["content"]
    if chat_channel_content["status"] == "CLOSE":
        print("status == CLOSE")
        sys.exit(0)
    else:
        chat_channel_id = chat_channel_content["chatChannelId"]
    # accTkn을 발급받아야한다.
    access_token = requests.get(
        f"https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId={chat_channel_id}&chatType=STREAMING"
    ).json()["content"]["accessToken"]
    websocket.enableTrace(False)
    # chating 가져오기
    ws = websocket.WebSocketApp(
        "wss://kr-ss2.chat.naver.com/chat",
        on_open=partial(on_open, cid=chat_channel_id, accTkn=access_token),
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever()
