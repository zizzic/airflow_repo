## live_viewer glue script template

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

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
# 데이터 가공
datasource_df = datasource.toDF()

chzzk_source = datasource_df.select("stream_data.chzzk").select(explode("chzzk"))
afreeca_source = datasource_df.select("stream_data.afreeca").select(explode("afreeca"))

# chzzk_source.printSchema()
chzzk_df = chzzk_source.select(
    col("col.streamer_id").alias("STREAMER_ID"),
    col("col.content.liveID").alias("BROADCAST_ID"),
    col("col.content.liveTitle").alias("BROADCAST_TITLE"),
    col("col.content.liveCategoryValue").alias("GAME_CODE"),
    col("col.content.concurrentUserCount").alias("VIEWER_NUM"),
)
# add platform
chzzk_df = chzzk_df.withColumn("PLATFORM", lit("chzzk"))
# chzzk_df.show()

# afreeca_source.printSchema()
afreeca_df = afreeca_source.select(
    col("col.streamer_id").alias("STREAMER_ID"),
    col("col.live_status.BNO").alias("BROADCAST_ID"),
    col("col.live_status.TITLE").alias("BROADCAST_TITLE"),
    col("col.live_status.CATE").alias("GAME_CODE"),
    col("col.broad_info.broad.current_sum_viewer").alias("VIEWER_NUM"),
)
afreeca_df = afreeca_df.withColumn("PLATFORM", lit("afreeca"))
# afreeca_df.show(truncate=False)


result_df = chzzk_df.join(
    afreeca_df,
    [
        "STREAMER_ID",
        "BROADCAST_ID",
        "BROADCAST_TITLE",
        "GAME_CODE",
        "PLATFORM",
        "VIEWER_NUM",
    ],
    "outer",
)

# 스키마 정보를 로깅
print("Schema Information:")
result_df.printSchema()

dynamicframe = DynamicFrame.fromDF(result_df, glueContext, "dynamicframe")


# Parquet으로 변환하여 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=dynamicframe,
    connection_type="s3",
    connection_options={"path": "{{ output_path }}"},
    format="parquet",
    transformation_ctx="datasource",
)

# Job Bookmark의 상태를 최종적으로 커밋
job.commit()
