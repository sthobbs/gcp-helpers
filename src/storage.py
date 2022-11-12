# TODO(user): Need to provide service account key in "./service_account_key.json"

from google.cloud import storage
from google.oauth2 import service_account
from glob import glob
import os
from pathlib import Path
from src.logger import Logger


class Storage():
    """Google Cloud Storage API helper class"""

    def __init__(self, **kwargs):
        """Initializes a client for Google Cloud Storage API"""

        # set attributes
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

        # set up logger
        self.logger = Logger(self.project_id).logger

    def download_blob(self, gcs_src_path, local_dest_path):
        """
        Downloads a blob from the bucket.

        Parameters
        ----------
        gcs_src_path : str
            The path to the blob to download, including the bucket name.
            Don't include the project name or bucket name, e.g. "my_blob.txt".
        local_dest_path : str
            The path to the local file where the blob's contents are to be
            downloaded, e.g. "./my_file_path.txt".
        """

        blob = self.bucket.blob(gcs_src_path)
        blob.download_to_filename(local_dest_path)
        self.logger.info(f"Blob {gcs_src_path} downloaded to {local_dest_path}.")

    def upload_file(self, local_src_path, gcs_dest_path):
        """
        Uploads a file to the bucket.

        Parameters
        ----------
        local_src_path : str
            The path to the local file to upload, e.g. "./my_file_path.txt".
        gcs_dest_path : str
            The path to the blob to upload to, not including the project or
            bucket name, e.g. "my_blob.txt".
        """

        blob = self.bucket.blob(gcs_dest_path)
        self.logger.info(f"Uploading {local_src_path} to {gcs_dest_path}...")
        blob.upload_from_filename(local_src_path)

    def upload_dir_recursive(self, local_src_dir, gcs_dest_dir):
        """
        Uploads a directory recursively to the bucket.

        Parameters
        ----------
        local_src_dir : str
            The path to the local directory to upload, e.g. "./my_dir".
        gcs_dest_dir : str
            The path to the directory to upload to, not including the project or
            bucket name, e.g. "my_dir".
        """

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
        """
        Lists all the blobs in the bucket that begin with the prefix.

        Parameters
        ----------
        prefix : str
            The prefix, if any, of the blobs to list. For example, to list all
            blobs in the ``public`` directory, pass ``prefix=public``.
            If not provided, all blobs in the bucket are listed.
        delimiter : str
            The delimiter, if any, to use when listing blobs. For example, to
            list all blobs in ``public/`` and ``public/images/``, pass
            ``prefix=public/`` and ``delimiter=/``. The delimiter is a character
            or string used to group blobs. The delimiter may be a single character
            (e.g. ``/``) or a multi-character string (e.g. ``/-/``). When the
            delimiter is provided, the list of blobs returned will only contain
            blobs whose names, aside from the prefix, do not contain the delimiter.
            Objects whose names, aside from the prefix, contain the delimiter will
            have their name, truncated after the delimiter, returned in
            :attr:`Blob.prefixes`. Duplicate prefixes are omitted.
        """

        self.logger.info(f"Getting list of blobs with prefix {prefix}")
        blobs = self.bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        return [b.name for b in blobs]
