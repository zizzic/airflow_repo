import certifi
import json
import ssl
import base64
import asyncio
import requests
import websockets
import sys

# WSS 연결 위한 SSL 설정
ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(certifi.where())
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


def decode(bytes):
    test = bytes.split(b"\x0c")
    res = []
    for i in test:
        res.append(str(i, "utf-8"))
    if res[1] != "-1" and res[1] != "1" and "|" not in res[1]:
        if len(res) > 5:
            print(res[1], "\t| ", res[2], "|", res[6])
        else:
            print(res)
            pass


def player_live_api(bno, bid):
    # type=aid 일 경우 aid(.A32~~~)불러옴
    data = {
        "bid": bid,
        "bno": bno,
        "type": "live",
        "confirm_adult": "false",
        "player_type": "html5",
        "mode": "landing",
        "from_api": "0",
        "pwd": "",
        "stream_type": "common",
        "quality": "HD",
    }
    res = requests.post(
        f"https://live.afreecatv.com/afreeca/player_live_api.php?bjid={bid}", data=data
    ).json()
    CHDOMAIN = res["CHANNEL"]["CHDOMAIN"].lower()
    CHATNO = res["CHANNEL"]["CHATNO"]
    FTK = res["CHANNEL"]["FTK"]
    TITLE = res["CHANNEL"]["TITLE"]
    BJID = res["CHANNEL"]["BJID"]
    CHPT = str(int(res["CHANNEL"]["CHPT"]) + 1)
    return CHDOMAIN, CHATNO, FTK, TITLE, BJID, CHPT


async def connect(url):
    BNO = str(url.split("/")[-1])
    BID = str(url.split("/")[-2])
    CHDOMAIN, CHATNO, FTK, TITLE, BJID, CHPT = player_live_api(BNO, BID)

    KEY = ""
    if len(BJID) == 5:
        KEY = "80"
    elif len(BJID) == 6:
        KEY = "81"
    elif len(BJID) == 7:
        KEY = "82"
    elif len(BJID) == 8:
        KEY = "83"
    elif len(BJID) == 9:
        KEY = "84"
    elif len(BJID) == 10:
        KEY = "85"
    elif len(BJID) == 11:
        KEY = "86"
    elif len(BJID) == 12:
        KEY = "87"

    handshake = f"	000200032200{CHATNO}{FTK}0log&set_bps=8000&view_bps=1000&quality=normal&uuid=cb2f85456a1844db7c6b5b99d618681e&geo_cc=KR&geo_rc=11&acpt_lang=ko_KR&svc_lang=ko_KR&subscribe=0&lowlatency=0pwdauth_infoNULLpver1access_systemhtml5"

    async with websockets.connect(
        f"wss://{CHDOMAIN}:{CHPT}/Websocket/{BID}",
        subprotocols=["chat"],
        ssl=ssl_context,
        ping_interval=None,
    ) as websocket:
        # 핸드쉐이크
        # await websocket.send(secret_1)
        await websocket.send("	00010000060016")
        data = await websocket.recv()
        # print(data)
        await websocket.send(handshake)
        # 이후부터 채팅내용 받아와짐
        while True:
            try:
                data = await websocket.recv()
                decode(data)
            except Exception as e:
                print("ERROR:", e)
                # sys.exit(0)
                # break


url = "https://play.afreecatv.com/fbalstjr1234/253965341"
asyncio.get_event_loop().run_until_complete(connect(url))
