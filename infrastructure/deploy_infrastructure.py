
from infrastructure.glue import deploy_glue_jobs
from infrastructure.roles import deploy_roles
from infrastructure.s3 import S3Manager
from utils import file_operations
import s3fs
import boto3

PATH_TO_TEST_JOB_JSON = "infrastructure/jobs.json"
PATH_TO_SCRIPTS_AND_DATA = "infrastructure/pipedream/main"

def main():
    session = boto3.session.Session()
    glue_client = session.client("glue")
    sfn_client = session.client("stepfunctions")
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    s3_filesystem = s3fs.S3FileSystem()
    s3_manager = S3Manager(s3_client, s3_filesystem)

    bucket_name = "aws-glue-pipedream-test-bucket"
    #PATH_TO_SCRIPTS_AND_DATA = ""

    s3_manager.create_bucket(bucket_name)
    print(f"Created {bucket_name}")

    s3_manager.upload_files(bucket_name, PATH_TO_SCRIPTS_AND_DATA)
    print(f"Uploaded files to {bucket_name} bucket")

    json_object = file_operations.read_json_file(PATH_TO_TEST_JOB_JSON)
    deploy_glue_jobs(glue_client, json_object)

    deploy_roles()
    
    print("All glue jobs successfully created")



if __name__ == '__main__':
    main()