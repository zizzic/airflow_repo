import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, explode

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
game_rating_datasource = datasource.toDF()

# 최상위 레벨의 key를 중심으로 explode하기
df = game_rating_datasource.select(
    explode(game_rating_datasource.raw_game_rating).alias("raw_game_rating")
)

df = df.select(
    col("raw_game_rating.game_id").alias("GAME_ID"),
    lit("{{ collect_date }}").alias("COLLECT_DATE"),
    col("raw_game_rating.recent_positive_num").alias("RECENT_POSITIVE_NUM"),
    col("raw_game_rating.recent_positive_percent").alias("RECENT_POSITIVE_PERCENT"),
    col("raw_game_rating.all_positive_num").alias("ALL_POSITIVE_NUM"),
    col("raw_game_rating.all_positive_percent").alias("ALL_POSITIVE_PERCENT"),
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
