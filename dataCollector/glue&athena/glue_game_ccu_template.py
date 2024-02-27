import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, when, udf, explode
from pyspark.sql.types import StringType

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

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
game_ccu_datasource = datasource.toDF()

# 최상위 레벨의 key를 중심으로 explode하기
df = game_ccu_datasource.select(
    explode(game_ccu_datasource.raw_game_ccu).alias("raw_game_ccu"),
    col("collect_time").alias("COLLECT_TIME"),
)

df = df.select(
    col("raw_game_ccu.game_id").alias("GAME_ID"),
    col("COLLECT_TIME"),
    col("raw_game_ccu.player_count").alias("GAME_CCU"),
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
