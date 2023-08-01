class S3Manager:
    def __init__(self, s3_client, s3_filesystem):
        self.s3_client = s3_client
        self.s3_filesystem = s3_filesystem

    def create_bucket(self, bucket_name):
        try:
            response = self.s3_client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
            )
        except self.s3_client.exceptions.BucketAlreadyExists:
            print("Bucket already exists")
        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass

    def upload_files(self, bucket_name, local_directory):
        self.s3_filesystem.put(local_directory, bucket_name, recursive=True)

    def delete_bucket(self, bucket_name):
        self.s3_filesystem.rm(f"{bucket_name}/pipedream/main/", recursive=True)
        response = self.s3_client.delete_bucket(Bucket=bucket_name)
        return response

