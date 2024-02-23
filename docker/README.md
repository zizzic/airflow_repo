## zdd-airflow Image

### Image for PROD
Build a docker image with buildx

```shell
AIRFLOW_VERSION=2.7.2
REPOSITORY=nzkangho/airflow
VERSION=0.1
docker buildx build -t ${REPOSITORY}:${AIRFLOW_VERSION}-${VERSION} --no-cache \
      --build-arg AIRFLOW_VERSION=${AIRFLOW_VERSION} \
      --push \
      -f docker/Dockerfile \
      .
```

### Image for DEV(+ interpreter)
Build a docker image with buildx

```shell
AIRFLOW_VERSION=2.7.2
REPOSITORY=nzkangho/airflow
VERSION=0.1
docker buildx build -t ${REPOSITORY}:${AIRFLOW_VERSION}-${VERSION}-dev --no-cache \
      --build-arg AIRFLOW_VERSION=${AIRFLOW_VERSION} \
      --build-arg VERSION=${VERSION} \
      --push \
      -f docker/dev.Dockerfile \
      .
```