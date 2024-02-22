import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, when, udf, explode
from pyspark.sql.types import IntegerType, StringType

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def remove_krw(price):
    return price.replace('₩ ', '')


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

# 전처리 하기
game_price_datasource = datasource.toDF()

remove_krw_udf = udf(remove_krw, StringType())

df = game_price_datasource.select(
    col("col.data.steam_appid").alias("GAME_ID"),
    lit("{{ collect_date }}").alias("COLLECT_DATE"),
    when(col("col.data.price_overview").isNull(), 0)
        .otherwise(remove_krw_udf(col("col.data.price_overview.final_formatted")).cast(IntegerType()))
        .alias("CURRENT_PRICE"),
    when(col("col.data.price_overview").isNull(), 'F')
        .otherwise(when(col("col.data.price_overview.discount_percent") == 0, 'F').otherwise('T'))
        .alias("IS_DISCOUNT")
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
