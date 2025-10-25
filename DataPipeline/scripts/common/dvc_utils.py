"""DVC operations"""

import os
import shutil
import subprocess
from pathlib import Path


class DVCManager:
    """Simple DVC operations without local data storage."""
    
    @staticmethod
    def track(local_path: str, commit_msg: str = None, 
              keep_local: bool = False) -> str:
        """
        Track file with DVC, optionally remove local copy.
        
        Args:
            local_path: File to track (e.g., "data/RAG/merged/combined_latest.jsonl")
            commit_msg: Git commit message
            keep_local: If False, deletes file after tracking
            
        Returns:
            Path to .dvc pointer file
        """
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # DVC add (creates .dvc file, pushes to remote)
        subprocess.run(['dvc', 'add', local_path], check=True, capture_output=True)
        subprocess.run(['dvc', 'push'], check=True, capture_output=True)
        
        # Remove local file to save space
        if not keep_local and os.path.exists(local_path):
            os.remove(local_path)
        
        # Git commit the .dvc pointer
        dvc_file = f"{local_path}.dvc"
        subprocess.run(['git', 'add', dvc_file], check=True, capture_output=True)
        
        if commit_msg:
            subprocess.run(
                ['git', 'commit', '-m', commit_msg],
                check=False,  # Don't fail if nothing to commit
                capture_output=True
            )
        
        return dvc_file
    
    @staticmethod
    def pull(dvc_path: str) -> str:
        """
        Pull file from DVC remote to local.
        
        Args:
            dvc_path: Path to .dvc file or tracked file
            
        Returns:
            Local file path
        """
        if not dvc_path.endswith('.dvc'):
            dvc_path = f"{dvc_path}.dvc"
        
        subprocess.run(['dvc', 'pull', dvc_path], check=True, capture_output=True)
        
        # Return path to actual file (without .dvc extension)
        return dvc_path.replace('.dvc', '')