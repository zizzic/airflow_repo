# 스트리머의 게임 방송과 게임 흥행 사이의 관계

## 주제
실시간으로 스트리머의 게임 방송 데이터를 수집하여 스트리머가 게임 흥행에 미치는 파급력을 확인

## 활용 데이터
| source    | data                                                    | link |
|-----------|---------------------------------------------------------|------|
| Chzzk     | liveTitle, status, concurrentUserCount                  |  https://chzzk.naver.com/    |
| AfreecaTV | broadcastNo, title, category, currentSumViewer          |  https://www.afreecatv.com/    |
| Steam     | gameName, playerCount, discountPercent, positiveNum ... |  https://partner.steamgames.com/doc/home                              |


## 활용 기술 및 프레임 워크
| 분류 | 기술 |
| --- | --- |
| 사용 언어 (Programming Language) | Python, SQL |
| 클라우드 서비스 (Cloud Service) | GCP, AWS (Terraform) |
| 데이터 베이스 (Database) | AWS S3, AWS Redshift, AWS RDS(MySQL) |
| 프로세싱 (Data Processing) | Airflow, AWS Glue(Spark), AWS Athena, AWS Lambda |
| 시각화 (Visualization) | Grafana(dashboard service) |
| 협업 도구 (Collaborative Software) | GitHub, Notion, Slack |

![architecture](/img/architecture.png)

## 프로젝트 보고서
- https://poriz98.notion.site/f49a712bdfa547308bb8516e0c278b5e?pvs=4

## 프로젝트 세부 결과
### Infra
- 시간과 추후 데이터 파이프라인의 규모를 고려하여, 제한된 리소스 내에서도 분산 환경을 고려한 Airflow 환경을 추가로 성공적으로 구축하였습니다.
### Dashboard
![home](/img/dashboard1.png)
![streamer](/img/dashboard2.png)
![game](/img/dashboard3.png)

### 결론
- 이 프로젝트는 스트리밍 플랫폼의 스트리머와 게임에 대한 실시간 통계를 제공하기 위해 효율적인 데이터 수집, 변환, 분석 인프라를 구축하였다. 이를 위해 AWS와 같은 클라우드 서비스를 활용하였고, Airflow, Redshift, Athena 등의 다양한 데이터 엔지니어링 도구를 사용하였다.
- 이 프로젝트를 통해 수집된 데이터는 스트리머의 활동 패턴, 게임의 인기도 및 시청자 수와 같은 다양한 요소를 파악하는 데 도움이 되며, 스트리밍 플랫폼의 현재 상황을 실시간으로 파악하고, 스트리밍 산업의 트렌드를 이해하는 데 기여할 수 있다.
- 데이터 분석 결과는 대시보드를 통해 시각화되어 제공되며, 스트리머, 게임, 전체 플랫폼에 대한 다양한 정보를 포함하고 있다. 
사용자는 이 대시보드를 통해 스트리머와 게임의 동향을 쉽게 이해하고 분석할 수 있다. 이러한 정보는 플랫폼 운영자나 스트리머, 게임 제작사 등에게 유용한 인사이트를 제공하며, 비즈니스 전략 수립에 도움이 될 수 있다.

## 개선점 & 회고
- **Liked** 좋았던 점
    - 여러가지 AWS 리소스를 사용해보고, 익숙해질 수 있었다.
    - 데이터 파이프라인을 자동화 하는 과정을 심도 있게 고민하고 여러가지 기술을 사용해서 구현에 성공한 것
    - 시간 및 리소스 제한을 고려하여 GCP를 사용하기로 빠르게 결정한 것
- **Learned** 배운 점
    - 데이터 엔지니어링에 유효한 AWS 서비스들을 직접 사용해본 것
    - 데이터 처리 과정에 있어서 ERD 및 S3구조 등 사전 설계의 중요성
    - Github에 대한 여러가지 기능(Pull Request, Action, Issue, …) 사용
- **Lacked** 부족했던 점
    - 코드 리뷰 및 리팩토링에 대해 고민하고 작성한 시간이 부족
    - raw data compaction에 대한 DAG 개발 미진행
    - 스트리머와 게임 데이터의 크기가 작아서 빅데이터 처리까지 진행 불가
- **Longed for** 갈망했던 점 (하고 싶었으나 하지 못한 것)
    - 사용 가능한 리소스가 너무 부족
        - Airflow 컴포넌트 분리 등, 필요한 개발 환경을 조성하는데 너무 많은 시간을 소요
    - 프로젝트에 취지에 맞는 실시간성 데이터를 확보하지 못함
        - 데브코스 과정에서 학습한 기술들을 전반적으로 복습 및 활용함과 동시에 실시간성을 갖춘 데이터를 찾기 힘들었음
