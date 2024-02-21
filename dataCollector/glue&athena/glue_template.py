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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# S3에서 데이터를 읽어오는 부분
datasource = glueContext.create_dynamic_frame.from_options(
    's3',
    {'paths': ['{{ input_path }}'], 'recurse':True},
    format='json',
    transformation_ctx='datasource'
)

# Parquet으로 변환하여 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=datasource,
    connection_type='s3',
    connection_options={'path': '{{ output_path }}'},
    format='parquet',
    transformation_ctx='datasource'
)


# Job Bookmark의 상태를 최종적으로 커밋
job.commit()
