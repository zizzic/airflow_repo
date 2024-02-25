import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# <---------add functions----------->

# "PLATFORM" 컬럼을 기준으로 파티션을 구성
partitioned_df = result_df.repartition("PLATFORM")

# 파티션된 Spark DataFrame을 DynamicFrame으로 변환
partitioned_dynamic_frame = DynamicFrame.fromDF(partitioned_df, glueContext, "partitioned_dynamic_frame")


# Parquet으로 변환하여 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=datasource,
    connection_type="s3",
    connection_options={"path": "{{ output_path }}"},
    format="parquet",
    transformation_ctx="datasource",
)
