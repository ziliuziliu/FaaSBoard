"""
Script to upload a specified folder to S3
Usage: python upload_to_s3.py <folder_name>
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration parameters
S3_BUCKET_NAME = "ltruan"  # S3 bucket name
AWS_REGION = "ap-southeast-1"  # AWS region

def upload_folder_to_s3(local_folder_path, s3_prefix=""):
    """
    Upload a local folder to S3, preserving the directory structure
    
    Args:
        local_folder_path: Path to the local folder
        s3_prefix: S3 prefix path
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Check if folder exists
        if not os.path.isdir(local_folder_path):
            logger.error(f"Folder does not exist: {local_folder_path}")
            return False
        
        # Get base folder name
        base_folder_name = os.path.basename(local_folder_path)
        
        # Track upload statistics
        files_uploaded = 0
        
        # Traverse all files in the folder
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                
                # Calculate path relative to the parent directory
                relative_path = os.path.relpath(local_file_path, os.path.dirname(local_folder_path))
                
                # Construct S3 object key preserving the directory structure
                s3_object_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                
                # Upload file
                # Key format: <s3_prefix>/<base_folder_name>/<relative_path>
                # e.g., twitter-2010-balance/0/graph0.csr.in
                logger.info(f"Uploading file: {local_file_path} -> s3://{S3_BUCKET_NAME}/{s3_object_key}")
                s3_client.upload_file(local_file_path, S3_BUCKET_NAME, s3_object_key)
                files_uploaded += 1
        
        logger.info(f"Success: {files_uploaded} files from {base_folder_name} uploaded to S3 bucket {S3_BUCKET_NAME}")
        return True
    
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

def main():
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python upload_to_s3.py <folder_name>")
        sys.exit(1)
        
    # Get folder name
    folder_name = sys.argv[1]
    
    # Build complete folder path
    # Assuming script is run from FaaSBoard/script directory and data is in FaaSBoard/data
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    root_dir = script_dir.parent  # FaaSBoard root directory
    data_dir = root_dir / "data"  # Data directory
    target_folder_path = data_dir / folder_name  # Target folder
    
    # Upload folder
    success = upload_folder_to_s3(str(target_folder_path))
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()