# Wrapper around the Google Cloud Storage API, with some additional functionality.

# TODO(user): Need to provide service account key in "./service_account_key.json"

from google.cloud import storage
from google.oauth2 import service_account
from glob import glob
import os
from pathlib import Path


class Storage():

    def __init__(self, **kwargs):
        _kws = {'project_id', 'bucket_name'}
        self.__dict__.update({k: v for k, v in kwargs.items() if k in _kws})
        # pass service account key into credentials
        key_path = './service_account_key.json'
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = storage.Client(credentials=credentials, project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)

    def download_blob(self, gcs_src_path, local_dest_path):
        # gcs_src_path is a file path which doesn't include the project or bucket name
        # local_dest_path is a file path
        blob = self.bucket.blob(gcs_src_path)
        blob.download_to_filename(local_dest_path)
        print(f"Blob {gcs_src_path} downloaded to {local_dest_path}.")

    def upload_file(self, local_src_path, gcs_dest_path):
        # gcs_dest_path doesn't include the project or bucket name
        blob = self.bucket.blob(gcs_dest_path)
        print(f"Uploading {local_src_path} to {gcs_dest_path}...")
        blob.upload_from_filename(local_src_path)

    def upload_dir_recursive(self, local_src_dir, gcs_dest_dir):
        files_and_dirs = glob(f"{local_src_dir}/*")
        for file_or_dir in files_and_dirs:
            name = Path(file_or_dir).name
            parent_name = Path(file_or_dir).parent.name
            # directory case
            if os.path.isdir(file_or_dir):
                src_subdir = f"{local_src_dir}/{name}"
                dest_subdir = f"{gcs_dest_dir}/{parent_name}"
                self.upload_dir_recursive(src_subdir, dest_subdir)
            # file case
            else:
                dest = f"{gcs_dest_dir}/{parent_name}/{name}"
                self.upload_file(file_or_dir, dest)

    def list_blobs(self, prefix=None, delimiter=None):
        blobs = self.bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        return [b.name for b in blobs]
