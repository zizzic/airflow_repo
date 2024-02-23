Airflow for zizzic
---
Airflow DAG Codes for zizzic

# Package Management
[pip-tools](https://pypi.org/project/pip-tools/)를 사용하여,
- [requirements.in](requirements.in)
- [requirements-dev.in](requirements-dev.in)
에 필요한 package와 버전을 `PACKAGE==VERSION` 형태로 작성한 뒤
아래와 같이 pip-tools를 이용하여 필요한 package의 dependencies들이
- [requirements.txt](requirements.txt)
- [requirements-dev.txt](requirements-dev.txt)
에 명시되도록하고, 이를 통해 어떤 패키지의 dependency인지 알 수 있다.


# Development

Run code formatter [black](https://black.readthedocs.io/en/stable/) using Docker

```shell
VERSION=2.7.2-0.1
docker run --rm --volume $(pwd):/src --workdir /src nzkangho/airflow:${VERSION}-dev black .
```

Lint code using black

```shell
VERSION=2.7.2-0.1
docker run --rm --volume $(pwd):/src --workdir /src nzkangho/airflow:${VERSION}-dev black --check .
```

Run all test codes under `/tests`

```shell
VERSION=2.7.2-0.1
docker run --rm --volume $(pwd):/src --workdir /src nzkangho/airflow:${VERSION}-dev pytest . --color=yes -vv
```

# Docker Images

See [docker](./docker).
All scripts must be run from the project root. 

---
Maintainers @zizzic/programmers