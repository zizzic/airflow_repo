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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
job.init(args["JOB_NAME"], args)

input_path = args['input_path']
output_path = args['output_path']

# S3에서 데이터를 읽어오는 부분
datasource = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [input_path], "recurse": True},
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
    col("col.current_time").alias("COLLECT_TIME_CHZZK"),
    col("col.content.followerCount").alias("CHZ_FOLLOWER_NUM"),
)

# afreeca_source.printSchema()
afreeca_df = afreeca_source.select(
    col("col.streamer_id").alias("STREAMER_ID"),
    col("col.current_time").alias("COLLECT_TIME_AFREECA"),
    col("col.broad_info.station.upd.fan_cnt").alias("AFC_FOLLOWER_NUM"),
)

# Perform the outer join
result_df = chzzk_df.join(afreeca_df, on='STREAMER_ID', how='outer')

# Use when() and otherwise() to select the appropriate COLLECT_TIME
result_df = result_df.withColumn(
    "COLLECT_TIME",
    when(
        col("COLLECT_TIME_CHZZK") != "", col("COLLECT_TIME_CHZZK")
    ).otherwise(
        col("COLLECT_TIME_AFREECA")
    )
).select(
    col("STREAMER_ID"),
    col("COLLECT_TIME"),
    col("CHZ_FOLLOWER_NUM"),
    col("AFC_FOLLOWER_NUM")
)


# 스키마 정보를 로깅
print("Schema Information:")
result_df.printSchema()

coalesce_df = result_df.coalesce(1)

# 파티션된 Spark DataFrame을 DynamicFrame으로 변환
dynamic_frame = DynamicFrame.fromDF(coalesce_df, glueContext, "dynamic_frame")


# Parquet으로 변환하여 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    transformation_ctx="dynamic_frame",
)