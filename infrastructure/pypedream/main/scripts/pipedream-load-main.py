#Load
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','SILVER_DATA_SOURCE','GOLD_DATA_SINK'])
source_s3_path = args['SILVER_DATA_SOURCE']
target_s3_path = args['GOLD_DATA_SINK']

target_format = "parquet"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def read_table(location):
    """Read a specified table from the cluster using the provided credentials."""
    df = spark.read.parquet(location)
    return df


def write_table(table_name, table_data):
    """Write a table to the target s3 location using the table name as a path."""
    # table_data = table_data.withColumn("ExtractDate",F.current_date())
    datasink = glueContext.write_dynamic_frame.from_options(
        frame = DynamicFrame.fromDF(table_data,glueContext,table_name), 
        connection_type = "s3", 
        connection_options = {"path": target_s3_path }, #, "partitionKeys": ["ExtractDate"] 
        format = target_format
    )   
    
def transform_df(df):
    df = df.distinct()
    # .withColumn("catname",
    #     when(df.catname=="Classical", "Contemporary"))\
    # .withColumn("important", lit("no"))
    return df
    
df = read_table(source_s3_path)
# df = transform_df(df)
write_table("data",df)

job.commit()
