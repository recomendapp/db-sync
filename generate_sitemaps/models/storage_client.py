import boto3
from prefect.blocks.system import Secret

class StorageClient:
    def __init__(self):
        endpoint = self._get_secret("s3-endpoint")
        access_key = self._get_secret("s3-access-key")
        secret_key = self._get_secret("s3-secret-key")
        self.bucket = self._get_secret("s3-sitemap-bucket")
        
        # Boto3 Client for MinIO
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1"
        )

    def _get_secret(self, secret_name: str) -> str:
        try:
            return Secret.load(secret_name).get()
        except Exception as e:
            raise ValueError(f"S3 secret '{secret_name}' not found: {e}")

    def upload(self, path: str, content: bytes):
        self.client.put_object(
            Bucket=self.bucket,
            Key=path,
            Body=content,
            ContentType='application/xml',
            ContentEncoding='gzip'
        )

    def clean_excess_sitemaps(self, prefix: str, current_count: int):
        """
        Deletes sitemap files in the specified prefix that have an index >= current_count.
        """
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            return

        objects_to_delete = []
        for obj in response['Contents']:
            key = obj['Key']
            try:
                filename = key.split('/')[-1]
                if filename == 'index.xml.gz':
                    continue
                
                file_index = int(filename.split('.')[0])
                if file_index >= current_count:
                    objects_to_delete.append({'Key': key})
            except ValueError:
                pass

        if objects_to_delete:
            self.client.delete_objects(
                Bucket=self.bucket,
                Delete={'Objects': objects_to_delete}
            )