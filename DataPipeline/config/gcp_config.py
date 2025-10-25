"""GCP configuration."""

import os


class Config:
    """Configuration for MedScan AI pipeline."""
    
    # GCP
    PROJECT_ID = os.getenv("GCP_PROJECT_ID", "medscanai-476203")
    REGION = os.getenv("GCP_REGION", "us-central1")
    
    # Buckets
    DATA_BUCKET = f"{PROJECT_ID}-rag-data"
    DVC_BUCKET = f"{PROJECT_ID}-dvc-remote"
    MODEL_BUCKET = f"{PROJECT_ID}-models"
    
    # Service Account
    SA_EMAIL = f"rag-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com"
    
    # Paths
    @classmethod
    def gcs_path(cls, *parts):
        """Build GCS path."""
        return f"gs://{cls.DATA_BUCKET}/" + "/".join(parts)


config = Config()