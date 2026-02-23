import os
import json
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


class CloudStorageService:
    def __init__(self):
        self.bucket_name = os.getenv("S3_BUCKET")

        if not self.bucket_name:
            raise RuntimeError("S3_BUCKET not set")

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

    # ✅ Upload JSON snapshot
    def upload_json(self, key: str, data: dict):
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data),
                ContentType="application/json",
            )
            print(f"☁️ Uploaded to S3: {key}")
        except ClientError as e:
            print(f"❌ S3 upload failed: {e}")
            raise

    # ✅ Download JSON snapshot
    def download_json(self, key: str):
        try:
            response = self.s3.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )
            return json.loads(response["Body"].read())
        except ClientError as e:
            print(f"❌ S3 download failed: {e}")
            raise