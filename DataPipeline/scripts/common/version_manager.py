"""Version management - tracks pipeline artifacts."""

import json
from datetime import datetime
from typing import Dict, List, Optional
from google.cloud import storage


class VersionManager:
    def __init__(self, bucket_name: str, pipeline: str = "rag"):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name
        self.pipeline = pipeline
        self.manifest_path = f"metadata/version_manifest_{pipeline}.json"
    
    def get_manifest(self) -> Dict:
        """Load manifest, create if doesn't exist."""
        try:
            blob = self.bucket.blob(self.manifest_path)
            return json.loads(blob.download_as_text())
        except:
            return {
                "created_at": datetime.now().isoformat(),
                "current_version": 0,
                "pipeline": self.pipeline,
                "versions": []
            }
    
    def save_manifest(self, manifest: Dict) -> None:
        blob = self.bucket.blob(self.manifest_path)
        blob.upload_from_string(json.dumps(manifest, indent=2))
    
    def next_version(self) -> int:
        """Get next version number."""
        return self.get_manifest()['current_version'] + 1
    
    def register(self, version_type: str, files: Dict[str, str], 
                 metadata: Optional[Dict] = None) -> int:
        """Register new version and return version number."""
        manifest = self.get_manifest()
        version = self.next_version()
        
        manifest['versions'].append({
            "version": version,
            "type": version_type,
            "timestamp": datetime.now().isoformat(),
            "files": files,
            "metadata": metadata or {}
        })
        
        manifest['current_version'] = version
        self.save_manifest(manifest)
        
        print(f"✓ Version {version} registered ({version_type})")
        return version
    
    def get_latest_file(self, file_type: str) -> Optional[str]:
        """Get GCS path of latest file for given type."""
        manifest = self.get_manifest()
        
        for v in reversed(manifest['versions']):
            if file_type in v.get('files', {}):
                return v['files'][file_type]
        return None