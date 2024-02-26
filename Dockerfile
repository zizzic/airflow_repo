# 기본 이미지 지정. 버전은 2.7.2
FROM apache/airflow:2.7.2-python3.11

# root 사용자로 전환 (필요한 경우)
USER root

# 패키지 목록 업데이트 및 필요한 패키지 설치
RUN apt-get update && \
    apt-get install -y \
    vim \
    && rm -rf /var/lib/apt/lists/*

# 이후 필요한 작업 수행. 예를 들어, 환경 설정 변경, 파일 복사 등
