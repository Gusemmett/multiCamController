#!/usr/bin/env python3

"""
S3Controller - Simple S3 Upload Management

Handles automatic upload of multiCam recording files to S3 with support for multiple file types.
Designed for public write buckets to keep configuration simple.

Supported file types: .mp4, .zip, .mov, .avi, .m4v, and any other file types from cameras.
"""

import boto3
import os
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError
from botocore import UNSIGNED
from botocore.config import Config

class S3Controller:
    """Simple S3 controller for uploading multiCam files to a public write bucket"""

    def __init__(self, bucket_name, region="us-east-1"):
        """
        Initialize S3 controller

        Args:
            bucket_name (str): Name of the S3 bucket
            region (str): AWS region for the bucket
        """
        self.bucket_name = bucket_name
        self.region = region

        try:
            # Initialize S3 client for public bucket - no credentials needed
            self.s3_client = boto3.client(
                's3',
                region_name=region,
                config=Config(signature_version=UNSIGNED)
            )
            print(f"✅ S3 Controller initialized for public bucket: {bucket_name}")
        except Exception as e:
            print(f"⚠️  S3 Controller initialization warning: {e}")
            self.s3_client = None

    def generate_session_folder(self):
        """
        Generate a unique session folder path for organizing uploads

        Returns:
            str: Folder path in format YYYY-MM-DD/HH-MM-SS/
        """
        now = datetime.now()
        date_folder = now.strftime("%Y-%m-%d")
        time_folder = now.strftime("%H-%M-%S")
        return f"{date_folder}/{time_folder}/"

    def get_file_extension(self, file_path):
        """
        Get file extension for proper content type detection

        Args:
            file_path (str): Path to the file

        Returns:
            str: File extension (e.g., '.mp4', '.zip')
        """
        return Path(file_path).suffix.lower()

    def get_content_type(self, file_extension):
        """
        Determine appropriate content type for S3 upload

        Args:
            file_extension (str): File extension

        Returns:
            str: MIME content type
        """
        content_types = {
            '.mp4': 'video/mp4',
            '.mov': 'video/quicktime',
            '.avi': 'video/x-msvideo',
            '.m4v': 'video/x-m4v',
            '.zip': 'application/zip',
            '.tar': 'application/x-tar',
            '.gz': 'application/gzip',
            '.json': 'application/json',
            '.txt': 'text/plain'
        }

        return content_types.get(file_extension, 'application/octet-stream')

    def upload_file(self, local_path, s3_key):
        """
        Upload a single file to S3

        Args:
            local_path (str): Local file path
            s3_key (str): S3 object key (path in bucket)

        Returns:
            bool: True if upload successful, False otherwise
        """
        if not self.s3_client:
            print(f"❌ S3 client not available for upload: {local_path}")
            return False

        if not os.path.exists(local_path):
            print(f"❌ File not found: {local_path}")
            return False

        try:
            file_size = os.path.getsize(local_path)
            file_extension = self.get_file_extension(local_path)
            content_type = self.get_content_type(file_extension)

            print(f"📤 Uploading {Path(local_path).name} ({file_size / 1024 / 1024:.1f} MB) to S3...")

            # Force single-part upload (no multipart) using put_object
            with open(local_path, 'rb') as file_obj:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=file_obj,
                    ContentType=content_type,
                    Metadata={
                        'original-filename': Path(local_path).name,
                        'file-size': str(file_size),
                        'upload-timestamp': datetime.now().isoformat()
                    }
                )

            print(f"✅ Successfully uploaded: s3://{self.bucket_name}/{s3_key}")
            return True

        except ClientError as e:
            print(f"❌ AWS error uploading {local_path}: {e}")
            return False
        except NoCredentialsError:
            print(f"❌ AWS credentials not found for uploading {local_path}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error uploading {local_path}: {e}")
            return False

    def upload_batch(self, file_paths, custom_folder=None):
        """
        Upload multiple files to S3 in a session folder

        Args:
            file_paths (list): List of local file paths to upload
            custom_folder (str, optional): Custom folder name, otherwise generates timestamp folder

        Returns:
            dict: Results with 'success': bool, 'uploaded_files': list, 'failed_files': list
        """
        if not file_paths:
            return {'success': True, 'uploaded_files': [], 'failed_files': []}

        session_folder = custom_folder or self.generate_session_folder()
        uploaded_files = []
        failed_files = []

        print(f"📤 Starting batch upload of {len(file_paths)} files to folder: {session_folder}")

        for file_path in file_paths:
            if not os.path.exists(file_path):
                print(f"⚠️  Skipping missing file: {file_path}")
                failed_files.append(file_path)
                continue

            # Generate S3 key: session_folder + filename
            filename = Path(file_path).name
            s3_key = f"{session_folder}{filename}"

            # Upload the file
            if self.upload_file(file_path, s3_key):
                uploaded_files.append(file_path)
            else:
                failed_files.append(file_path)

        success = len(failed_files) == 0

        if success:
            print(f"✅ Batch upload complete: {len(uploaded_files)} files uploaded to s3://{self.bucket_name}/{session_folder}")
        else:
            print(f"⚠️  Batch upload partial: {len(uploaded_files)} succeeded, {len(failed_files)} failed")

        return {
            'success': success,
            'uploaded_files': uploaded_files,
            'failed_files': failed_files,
            'session_folder': session_folder
        }

    def delete_local_files(self, file_paths):
        """
        Safely delete local files after successful upload

        Args:
            file_paths (list): List of local file paths to delete

        Returns:
            dict: Results with 'success': bool, 'deleted_files': list, 'failed_deletions': list
        """
        deleted_files = []
        failed_deletions = []

        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    deleted_files.append(file_path)
                    print(f"🗑️  Deleted local file: {Path(file_path).name}")
                else:
                    print(f"⚠️  File already missing: {file_path}")

            except Exception as e:
                print(f"❌ Failed to delete {file_path}: {e}")
                failed_deletions.append(file_path)

        success = len(failed_deletions) == 0

        if success:
            print(f"✅ Local cleanup complete: {len(deleted_files)} files deleted")
        else:
            print(f"⚠️  Local cleanup partial: {len(deleted_files)} deleted, {len(failed_deletions)} failed")

        return {
            'success': success,
            'deleted_files': deleted_files,
            'failed_deletions': failed_deletions
        }

    def upload_and_cleanup(self, file_paths, custom_folder=None):
        """
        Convenience method: upload files to S3 and delete local copies if successful

        Args:
            file_paths (list): List of local file paths
            custom_folder (str, optional): Custom folder name for uploads

        Returns:
            dict: Combined results from upload and cleanup operations
        """
        # Upload files
        upload_result = self.upload_batch(file_paths, custom_folder)

        if upload_result['success']:
            # Only delete files that were successfully uploaded
            cleanup_result = self.delete_local_files(upload_result['uploaded_files'])

            return {
                'upload_success': True,
                'cleanup_success': cleanup_result['success'],
                'session_folder': upload_result['session_folder'],
                'total_files': len(file_paths),
                'uploaded_count': len(upload_result['uploaded_files']),
                'deleted_count': len(cleanup_result['deleted_files'])
            }
        else:
            # Don't delete anything if upload failed
            return {
                'upload_success': False,
                'cleanup_success': False,
                'session_folder': upload_result.get('session_folder'),
                'total_files': len(file_paths),
                'uploaded_count': len(upload_result['uploaded_files']),
                'failed_count': len(upload_result['failed_files'])
            }

    def test_connection(self):
        """
        Test S3 connection and bucket access

        Returns:
            bool: True if can access bucket, False otherwise
        """
        if not self.s3_client:
            return False

        try:
            # Try to list objects in bucket (just checking access)
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except Exception as e:
            print(f"❌ S3 connection test failed: {e}")
            return False