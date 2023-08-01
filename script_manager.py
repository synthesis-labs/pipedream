from http import client
import boto3
from botocore.exceptions import ClientError

class ScriptManager:

    def __init__(self, s3_client):
        self.s3_client = s3_client

    def _get_bucket_name_and_key(self, s3_location):
        if not s3_location.startswith("s3://"):
            raise ValueError("s3_path is expected to start with 's3://', " "but was {}".format(s3_location))
        bucket_key = s3_location[len("s3://") :]
        bucket_name, key = bucket_key.split("/", 1)
        return (bucket_name, key)


    def copy_script_to_local(self, s3_location, script_location):
        (bucket_name, key) = self._get_bucket_name_and_key(s3_location)
        print("Downloading script", bucket_name, key)
        try:
            self.s3_client.meta.client.download_file(bucket_name, key, script_location)
        except Exception:
            print("Script not found")


    def upload_script(self, script_location, s3_location):
        # Upload the file
        (bucket_name, key) = self._get_bucket_name_and_key(s3_location)
        response = self.s3_client.meta.client.upload_file(script_location, bucket_name, key)


    def delete_path(self, location):
        print("deleting location:", location)
        (bucket_name, key) = self._get_bucket_name_and_key(location)
        bucket = self.s3_client.Bucket(bucket_name)
        bucket.objects.filter(Prefix=key).delete()


    def delete_script(self, location):
        print("deleting location:", location)
        (bucket_name, key) = self._get_bucket_name_and_key(location)
        
        self.s3_client.Object(bucket_name, key).delete()

