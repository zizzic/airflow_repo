ARG AIRFLOW_VERSION=2.7.2

FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.11

# root 사용자로 전환 (필요한 경우)
USER root

RUN apt-get update -y

# 패키지 목록 업데이트 및 필요한 패키지 설치
# For python mysqlclient, see https://pypi.org/project/mysqlclient/
RUN apt-get install -y \
      default-libmysqlclient-dev \
      build-essential \
      pkg-config \
      libkrb5-dev

# Remove all caches installed by security hardening
RUN rm -rf /var/lib/apt/lists/* \
    && apt-get autoremove -yqq --purge \
    && apt-get clean

# 이후 필요한 작업 수행. 예를 들어, 환경 설정 변경, 파일 복사 등
USER airflow

# Upgrade PIP
RUN python -m pip install --no-cache-dir --upgrade pip

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && rm -f requirements.txt