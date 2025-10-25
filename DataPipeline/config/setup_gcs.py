"""GCS bucket setup for MedScan AI."""

import subprocess
import json
from pathlib import Path
from typing import Tuple


class GCSSetup:
    """Setup GCS buckets and folder structure."""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.data_bucket = f"{project_id}-rag-data"
        self.dvc_bucket = f"{project_id}-dvc-remote"
        self.model_bucket = f"{project_id}-models"
        
    def run_command(self, cmd: str) -> Tuple[bool, str]:
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                check=True
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            return False, e.stderr
    
    def create_bucket(self, bucket_name: str, storage_class: str = "STANDARD"):
        """Create GCS bucket."""
        print(f"\nCreating bucket: {bucket_name}")
        
        cmd = f"""
        gsutil mb \
            -p {self.project_id} \
            -c {storage_class} \
            -l {self.region} \
            gs://{bucket_name} 2>&1
        """
        
        success, output = self.run_command(cmd)
        if success or "already exists" in output.lower():
            print(f"Bucket ready")
            return True
        else:
            print(f"Error: {output}")
            return False
    
    def create_rag_folder_structure(self):
        print(f"\nCreating RAG folder structure")
        
        folders = [
            # Raw data
            "RAG/raw_data/baseline/",
            "RAG/raw_data/incremental/",
            
            # Merged data
            "RAG/merged/",
            
            # Validation (TFDV outputs)
            "RAG/validation/baseline/",      # Baseline schema/stats
            "RAG/validation/runs/",          # Each validation run
            "RAG/validation/latest/",        # Latest validation results
            
            # Processed data
            "RAG/chunked_data/",
            "RAG/index/",
            
            # Metadata
            "metadata/",
            
            # Staging
            "staging/",
        ]
        
        self._create_folders(self.data_bucket, folders)
    
    def _create_folders(self, bucket_name: str, folders: list):
        for folder in folders:
            placeholder = f"/tmp/.keep_{folder.replace('/', '_')}"
            Path(placeholder).touch()
            
            cmd = f"gsutil cp {placeholder} gs://{bucket_name}/{folder}.keep 2>&1"
            self.run_command(cmd)
            print(f"{folder}")
            
            Path(placeholder).unlink(missing_ok=True)
        
        # Remove .keep files
        cmd = f"gsutil -m rm 'gs://{bucket_name}/**/.keep' 2>&1 || true"
        self.run_command(cmd)
    
    def create_version_manifests(self):
        print(f"\n📋 Creating version manifests")
        
        manifest = {
            "created_at": datetime.now().isoformat(),
            "current_version": 0,
            "pipeline": "rag",
            "versions": []
        }
        
        temp_file = "/tmp/version_manifest_rag.json"
        with open(temp_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        cmd = f"gsutil cp {temp_file} gs://{self.data_bucket}/metadata/version_manifest_rag.json"
        self.run_command(cmd)
        Path(temp_file).unlink()
        
        print(f"Manifest created")
    
    def verify_setup(self):
        print(f"\n🔍 Verifying setup")
        
        cmd = f"gsutil ls gs://{self.data_bucket}/"
        success, output = self.run_command(cmd)
        
        if success and output:
            print(f"\n  Folders in {self.data_bucket}:")
            for line in output.strip().split('\n')[:15]:
                if line:
                    folder = line.replace(f"gs://{self.data_bucket}/", "").rstrip('/')
                    print(f" {folder}/")
    
    def run_complete_setup(self):
        """Execute complete setup."""
        print("=" * 70)
        print("GCS SETUP - MedScan AI RAG Pipeline")
        print("=" * 70)
        
        self.create_bucket(self.data_bucket)
        self.create_bucket(self.dvc_bucket)
        self.create_bucket(self.model_bucket)
        
        self.create_rag_folder_structure()
        
        self.create_version_manifests()
        self.verify_setup()
        
        print("\n" + "=" * 70)
        print("✅ GCS SETUP COMPLETE!")
        print("=" * 70)
        print(f"\nBuckets:")
        print(f"  • Data: gs://{self.data_bucket}")
        print(f"  • DVC:  gs://{self.dvc_bucket}")
        print(f"  • Models: gs://{self.model_bucket}")


if __name__ == "__main__":
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--region', default='us-central1')
    args = parser.parse_args()
    
    setup = GCSSetup(args.project_id, args.region)
    setup.run_complete_setup()