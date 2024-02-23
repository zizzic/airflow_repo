ARG AIRFLOW_VERSION=2.7.2
ARG VERSION=0.1

FROM nzkangho/airflow:${AIRFLOW_VERSION}-${VERSION}

WORKDIR tmp

COPY --chown=airflow:airflow requirements-dev.txt requirements-dev.txt
#COPY --chown=airflow:airflow dataCollector dataCollector
#COPY --chown=airflow:airflow dags dags
#COPY --chown=airflow:airflow tests tests
#COPY --chown=airflow:airflow plugins plugins

RUN pip install --no-cache-dir -r requirements-dev.txt && \
    rm -f requirements-dev.txt

ENTRYPOINT []