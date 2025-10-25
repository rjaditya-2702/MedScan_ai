"""GCS utility functions."""

import json
import os
from pathlib import Path
from typing import List, Optional

from google.cloud import storage


class GCSManager:
    
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name
    
    def upload_file(self, local_path: str, gcs_path: str) -> str:
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"Uploaded to gs://{self.bucket_name}/{gcs_path}")
        return f"gs://{self.bucket_name}/{gcs_path}"
    
    def download_file(self, gcs_path: str, local_path: str) -> str:
        blob = self.bucket.blob(gcs_path)
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(local_path)
        print(f"Downloaded to {local_path}")
        return local_path
    
    def list_files(self, prefix: str = "", delimiter: str = None) -> List[str]:
        blobs = self.bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        return [blob.name for blob in blobs]
    
    def upload_jsonl(self, records: List[dict], gcs_path: str) -> str:
        temp_file = f"/tmp/{Path(gcs_path).name}"
        with open(temp_file, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        uri = self.upload_file(temp_file, gcs_path)
        os.remove(temp_file)
        return uri
    
    def download_jsonl(self, gcs_path: str) -> List[dict]:
        blob = self.bucket.blob(gcs_path)
        content = blob.download_as_text()
        
        records = []
        for line in content.strip().split('\n'):
            if line.strip():
                records.append(json.loads(line))
        return records
    
    def upload_json(self, data: dict, gcs_path: str) -> str:
        temp_file = f"/tmp/{Path(gcs_path).name}"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        uri = self.upload_file(temp_file, gcs_path)
        os.remove(temp_file)
        return uri
    
    def download_json(self, gcs_path: str) -> dict:
        blob = self.bucket.blob(gcs_path)
        content = blob.download_as_text()
        return json.loads(content)
    
    def file_exists(self, gcs_path: str) -> bool:
        blob = self.bucket.blob(gcs_path)
        return blob.exists()
    
    def copy_file(self, source_path: str, dest_path: str) -> str:
        source_blob = self.bucket.blob(source_path)
        self.bucket.copy_blob(source_blob, self.bucket, dest_path)
        return f"gs://{self.bucket_name}/{dest_path}"