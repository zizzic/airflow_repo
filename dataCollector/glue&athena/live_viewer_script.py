## live_viewer glue script template

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

# SparkContext와 GlueContext 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Job 초기화 (Job Bookmark 활성화 포함)
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
job.init(args["JOB_NAME"], args)

input_path = args["input_path"]
output_path = args["output_path"]

# S3에서 데이터를 읽어오는 부분
datasource = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [input_path], "recurse": True},
    format="json",
    transformation_ctx="datasource",
)

# 공통 스키마 정의
common_schema = StructType(
    [
        StructField("STREAMER_ID", StringType(), True),
        StructField("BROADCAST_ID", StringType(), True),
        StructField("LIVE_COLLECT_TIME", TimestampType(), True),
        StructField("BROADCAST_TITLE", StringType(), True),
        StructField("GAME_CODE", StringType(), True),
        StructField("VIEWER_NUM", IntegerType(), True),
        StructField("PLATFORM", StringType(), True),
    ]
)

# 데이터 가공
datasource_df = datasource.toDF()
try:
    chzzk_source = datasource_df.select("stream_data.chzzk").select(explode("chzzk"))

    chz_filtered_source = chzzk_source.filter(col("col.content.adult") == "false")

    # chzzk_source.printSchema()
    chzzk_df = chz_filtered_source.select(
        col("col.streamer_id").alias("STREAMER_ID"),
        col("col.content.liveID").alias("BROADCAST_ID"),
        col("col.current_time").alias("LIVE_COLLECT_TIME"),
        col("col.content.liveTitle").alias("BROADCAST_TITLE"),
        col("col.content.liveCategoryValue").alias("GAME_CODE"),
        col("col.content.concurrentUserCount").alias("VIEWER_NUM"),
    )
    chzzk_df = chzzk_df.withColumn("PLATFORM", lit("chzzk"))
except:
    chzzk_df = spark.createDataFrame([], schema=common_schema)

try:
    afreeca_source = datasource_df.select("stream_data.afreeca").select(
        explode("afreeca")
    )
    afc_filtered_source = afreeca_source.filter(
        col("col.broad_info.broad.broad_grade") < 19
    )

    afreeca_df = afc_filtered_source.select(
        col("col.streamer_id").alias("STREAMER_ID"),
        col("col.live_status.BNO").alias("BROADCAST_ID"),
        col("col.current_time").alias("LIVE_COLLECT_TIME"),
        col("col.live_status.TITLE").alias("BROADCAST_TITLE"),
        col("col.live_status.CATE").alias("GAME_CODE"),
        col("col.broad_info.broad.current_sum_viewer").alias("VIEWER_NUM"),
    )
    afreeca_df = afreeca_df.withColumn("PLATFORM", lit("afreeca"))
except:
    afreeca_df = spark.createDataFrame([], schema=common_schema)


result_df = chzzk_df.union(afreeca_df)

# 스키마 정보를 로깅
print("Schema Information:")
result_df.printSchema()

# "PLATFORM" 컬럼을 기준으로 파티션을 구성
partitioned_df = result_df.repartition("PLATFORM")

# 파티션된 Spark DataFrame을 DynamicFrame으로 변환
partitioned_dynamic_frame = DynamicFrame.fromDF(
    partitioned_df, glueContext, "partitioned_dynamic_frame"
)


# Parquet으로 변환하여 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    transformation_ctx="datasource",
)
