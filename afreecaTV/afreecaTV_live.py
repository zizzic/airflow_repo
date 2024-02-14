import requests
import time

bjids = [
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
    "ghkdud1617",
    "legendhyuk",
    "maruko86",
    "ititit",
    "zw1948zw",
]

print(len(bjids))
data = {"type": "live"}
# 시작 시간 기록 (현재 시간을 초 단위로 반환)
start_time = time.time()


for bjid in bjids:
    data["bid"] = bjid
    url = f"https://live.afreecatv.com/afreeca/player_live_api.php?bjid={bjid}"
    # url = f"https://st.afreecatv.com/api/get_station_status.php?szBjId={bjid}"
    res = requests.post(url, data)
    print(len(res.text))
# 여기에 시간이 걸리는 작업을 수행하세요.

# 종료 시간 기록
end_time = time.time()

# 경과 시간 계산 (초 단위)
elapsed_time = end_time - start_time

print(f"경과 시간: {elapsed_time} 초")
