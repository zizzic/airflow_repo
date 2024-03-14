import sys

from pyspark.context import SparkContext
from pyspark.sql.functions import *
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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
job.init(args['JOB_NAME'], args)

input_path = args['input_path']
output_path = args['output_path']

# S3에서 데이터를 읽어오는 부분
datasource = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [input_path], "recurse": True},
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
    col("raw_game_ccu.game_id").cast("string").alias("GAME_ID"),
    col("COLLECT_TIME"),
    col("raw_game_ccu.player_count").alias("GAME_CCU"),
)

# 한국 시간대로 타임존을 출력하기 위해 하드코딩. 
# spark에 시간대를 지정하고 'Z'를 사용하는 방식도 존재함
df = df.withColumn(
    "COLLECT_TIME",
    date_format(
        to_timestamp("COLLECT_TIME", "yyyy-M-d H:m"),
        "yyyy-MM-dd'T'HH:mm:ss'+09:00'"
    )
)

dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Parquet으로 변환하여 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    transformation_ctx="dynamic_frame",
)


# Job Bookmark의 상태를 최종적으로 커밋
job.commit()
