# Transform
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, when
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','BRONZE_DATA_SOURCE','SILVER_DATA_SINK'])
source_s3_path = args['BRONZE_DATA_SOURCE']
target_s3_path = args['SILVER_DATA_SINK']

target_format = "parquet"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def read_table(location, head=0):
    """Read a specified table from the cluster using the provided credentials."""
    df = spark.read.parquet(location)
    if head:
        print(df.head(head))
    return df


def write_table(table_name, table_data):
    """Write a table to the target s3 location using the table name as a path."""
    # table_data = table_data.withColumn("ExtractDate",F.current_date())
    datasink = glueContext.write_dynamic_frame.from_options(
        frame = DynamicFrame.fromDF(table_data,glueContext,table_name), 
        connection_type = "s3", 
        connection_options = {"path": target_s3_path}, #, "partitionKeys": ["ExtractDate"] 
        format = target_format
    )

def transform_df(df):
    df = df.dropDuplicates()\
    # .withColumn("catname",
    #     when(df.catname=="Classical", "Contemporary"))\
    # .withColumn("important", lit("no")).limit(8)
    return df

df = read_table(source_s3_path, 10)
df = transform_df(df)
# print(df.head(10))
write_table("data",df)

job.commit()
