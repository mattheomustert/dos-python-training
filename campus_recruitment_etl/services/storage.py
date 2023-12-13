from google.cloud import storage


class GCSStorageClient:
    def __init__(self, bucket_name: str, official_client: storage.Client):
        self.bucket_name = bucket_name
        self.client = official_client
        self.bucket: storage.Bucket = self.client.bucket(bucket_name)

    def upload_to_gcs(self, local_file_path: str, gcs_file_path: str):
        blob = self.bucket.blob(gcs_file_path)
        blob.upload_from_filename(local_file_path)

    def list_files(self, prefix: str) -> list:
        blobs = self.client.list_blobs(self.bucket, prefix=prefix)

        return [blob.name for blob in blobs]

    def download_file(self, file_path: str) -> bytes:
        blob = self.bucket.blob(file_path)
        return blob.download_as_bytes()
