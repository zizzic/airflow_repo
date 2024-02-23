import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, when, udf, explode
from pyspark.sql.types import StringType

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def remove_krw(price):
    if price is None:
        return 0
    return int(price.replace("₩", "").replace(",", "").strip())


# SparkContext와 GlueContext 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Job 초기화 (Job Bookmark 활성화 포함)
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job.init(args["JOB_NAME"], args)

# S3에서 데이터를 읽어오는 부분
datasource = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": ["{{ input_path }}"], "recurse": True},
    format="json",
    transformation_ctx="datasource",
)

# 전처리를 위해 DF로 변환하기
game_price_datasource = datasource.toDF()

# 최상위 레벨의 key를 중심으로 explode하기
df = game_price_datasource.select(
    explode(game_price_datasource.raw_game_price).alias("raw_game_price")
)

# 게임 가격을 전처리하는 udf 정의하기
remove_krw_udf = udf(remove_krw, StringType())

# 이제 nested된 필드(key)들에 접근할 수 있음
df = df.select(
    col("raw_game_price.data.steam_appid").alias("GAME_ID"),
    lit("{{ collect_date }}").alias("COLLECT_DATE"),
    when(col("raw_game_price.data.price_overview").isNull(), 0)
    .otherwise(
        when(
            col("raw_game_price.data.price_overview.final_formatted").isNull(), 0
        ).otherwise(
            remove_krw_udf(col("raw_game_price.data.price_overview.final_formatted"))
        )
    )
    .alias("CURRENT_PRICE"),
    when(col("raw_game_price.data.price_overview").isNull(), "F")
    .otherwise(
        when(
            col("raw_game_price.data.price_overview.discount_percent") == 0, "F"
        ).otherwise("T")
    )
    .alias("IS_DISCOUNT"),
)

dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Parquet으로 변환하여 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": "{{ output_path }}"},
    format="parquet",
    transformation_ctx="dynamic_frame",
)


# Job Bookmark의 상태를 최종적으로 커밋
job.commit()
