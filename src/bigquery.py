# Wrapper around the BigQuery API

# TODO(user): Need to provide service account key in "./service_account_key.json"

import json
import pandas_gbq
import pandas as pd
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from collections import OrderedDict
from google.oauth2 import service_account


class BigQuery():

    def __init__(self, **kwargs):
        _kws = {'project_id', 'dataset_id', 'table_id', 'schema_json_path', 'location'}
        self.__dict__.update({k: v for k, v in kwargs.items() if k in _kws})

        self.full_dataset_id = f"{self.project_id}.{self.dataset_id}"
        self.full_table_id = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        if getattr(self, 'location', None) is None:
            self.location = 'US'
        if getattr(self, 'schema_json_path', None):
            self.table_schema = self.get_table_schema()

        # pass service account key into credentials
        key_path = './service_account_key.json'
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)

    def get_table_schema(self):
        if self.schema_json_path:
            try:
                with open(self.schema_json_path, 'r') as f:
                    schema = json.load(f, object_pairs_hook=OrderedDict)
            except Exception as e:
                print(f"error opening or parsing schema: {e}")
                raise
            if isinstance(schema, list):
                return schema
            elif isinstance(schema, dict):
                return [bigquery.SchemaField(k, v) for k, v in schema.items()]

    def dataset_exists(self):
        dataset_id = self.full_dataset_id
        try:
            self.client.get_dataset(dataset_id)
            print(f"Dataset {dataset_id} already exists")
            return True
        except NotFound:
            print(f"Dataset {dataset_id} is not found")
            return False

    def table_exists(self):
        table_id = self.full_table_id
        try:
            self.client.get_table(table_id)
            print(f"Table {table_id} already exists")
            return True
        except NotFound:
            print(f"Table {table_id} is not found")
            return False

    def create_dataset(self, exists_ok=False):
        dataset_id = self.full_dataset_id
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, exists_ok=exists_ok, timeout=30)
        print(f"Created dataset {dataset_id}")

    def delete_dataset(self):

        self.client.delete_dataset(
            self.full_dataset_id, delete_contents=True, not_found_ok=True
        )  # Make an API request.

        print(f"Deleted dataset '{self.dataset_id}'.")

    def create_table(self, partition_field=None, clustering_fields=None):
        # confirm table doesn't already exist
        if self.table_exists():
            return
        # confirm schema exists
        if not getattr(self, 'table_schema', None):
            print("No table_schema provided")
            raise ValueError("No table_schema provided")
        # create table representation
        table = bigquery.Table(self.full_table_id, schema=self.table_schema)
        # clusering fields
        if clustering_fields:
            table.clustering_fields = clustering_fields
        # partition field
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field)  # name of column to use for partitioning
        # create tabe
        table = self.client.create_table(table, timeout=30)
        print(f"Created table {self.full_table_id}")

    def delete_table(self):
        table_id = self.full_table_id
        self.client.delete_table(table_id, not_found_ok=True)
        print(f"Deleted table '{table_id}'.")

    def copy_table(self, dest_table, dest_dataset=None):
        if dest_dataset is None:
            dest_dataset = self.dataset_id
        dest_table_id = f"{self.project_id}.{dest_dataset}.{dest_table}"
        job = self.client.copy_table(self.full_table_id, dest_table_id)
        job.result()  # Wait for the job to complete.
        print(f"{self.full_table_id} was copied into {dest_table_id}.")

    def close(self):
        """Close the connection to the database."""
        self.client.close()
        return True

    def load_from_gcs(self,
                      gcs_uri,
                      source_format='CSV',
                      write_disposition='WRITE_TRUNCATE',
                      max_bad_records=0,
                      partition_field=None,
                      clustering_fields=None,
                      relaxed_schema=False):
        # check for valid inputs
        source_format = source_format.upper()
        assert source_format in ['CSV', 'JSON', 'AVRO'], \
            "source_format must be one of 'CSV', 'JSON', 'AVRO'"
        write_disposition = write_disposition.upper()
        assert write_disposition in ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'], \
            "write_disposition must be 'WRITE_TRUNCATE', 'WRITE_APPEND', or 'WRITE_EMPTY'"
        if clustering_fields is not None:
            assert isinstance(clustering_fields, list), \
                "clustering_fields must be None or a list of strings"

        # set up job config
        job_config = bigquery.LoadJobConfig()
        # set up job for source format
        if source_format == "CSV":
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
        elif source_format == "JSON":
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.max_bad_records = max_bad_records
        if source_format in {"CSV", "JSON"}:
            # set schema
            if getattr(self, 'table_schema', None):
                job_config.schema = self.table_schema
            else:
                job_config.autodetect = True
        elif source_format == "AVRO":
            job_config.source_format = bigquery.SourceFormat.AVRO
        elif source_format == "PARQUET":
            job_config.source_format = bigquery.SourceFormat.PARQUET
        # set up write disposition for job
        job_config.write_disposition = {
            'WRITE_TRUNCATE': bigquery.WriteDisposition.WRITE_TRUNCATE,
            'WRITE_APPEND': bigquery.WriteDisposition.WRITE_APPEND,
            'WRITE_EMPTY': bigquery.WriteDisposition.WRITE_EMPTY,
        }[write_disposition]
        # partition field
        if partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field)  # name of column to use for partitioning
        # set up clustering fields
        if clustering_fields is not None:
            job_config.clustering_fields = clustering_fields
        # relax schema restrictions
        if relaxed_schema and write_disposition == 'WRITE_APPEND':
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]
        # load data from gcs
        print(f"Loading data from {gcs_uri} into {self.full_table_id}")
        load_job = self.client.load_table_from_uri(
            gcs_uri, self.full_table_id, job_config=job_config
        )
        # wait for job to complete
        try:
            load_job.result()
        except BadRequest as e:
            for error in e.errors:
                print(error)
            raise
        destination_table = self.client.get_table(self.full_table_id)
        print(f"{destination_table.num_rows} rows in {self.full_table_id}.")

    def extract_to_gcs(self, gcs_uri, dest_format=None):
        # example gcs_uri: "gs://my-bucket/my_file_*.csv"

        job_config = bigquery.ExtractJobConfig()
        # set up destination format for job
        if dest_format is not None:
            dest_format = dest_format.upper()
            try:
                job_config.destination_format = {
                    'CSV': bigquery.DestinationFormat.CSV,
                    'JSON': bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
                    'AVRO': bigquery.DestinationFormat.AVRO,
                    'PARQUET': bigquery.DestinationFormat.PARQUET,
                }[dest_format]
            except KeyError:
                raise ValueError("dest_format must be one of None, \
                    'CSV', 'JSON', 'AVRO', or 'PARQUET'")
        else:
            if gcs_uri.upper().endswith('.JSON'):
                job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
            elif gcs_uri.upper().endswith('.AVRO'):
                job_config.destination_format = bigquery.DestinationFormat.AVRO
            elif gcs_uri.upper().endswith('.PARQUET'):
                job_config.destination_format = bigquery.DestinationFormat.PARQUET
            else:
                job_config.destination_format = bigquery.DestinationFormat.CSV
        # extract data to gcs
        print(f"Extracting data from {self.full_table_id} into {gcs_uri}")
        extract_job = self.client.extract_table(
            self.full_table_id, gcs_uri, location=self.location, job_config=job_config
        )
        # wait for job to complete
        extract_job.result()
        print(f"Data extracted from {self.full_table_id} into {gcs_uri}.")

    def load_from_dataframe(self, df):
        # load data from dataframe (errors if the table already exists,
        # although this can be changed)
        table_id = f"{self.dataset_id}.{self.table_id}"
        pandas_gbq.to_gbq(df, table_id, project_id=self.project_id)

    def query(self, query, dest_table_id=None, write_disposition='WRITE_TRUNCATE',
              relaxed_schema=False, partition_field=None, clustering_fields=None):
        # Note: partition_field and clustering_fields can only be used if don't conflict
        # the the destination table, even if write_disposition='WRITE_TRUNCATE'. If you
        # want to change the partitioning or clustering of a table, you must first delete
        # the table.

        # check for valid inputs
        write_disposition = write_disposition.upper()
        assert write_disposition in ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'], \
            "write_disposition must be 'WRITE_TRUNCATE', 'WRITE_APPEND', or 'WRITE_EMPTY'"
        # set up job config
        job_config = bigquery.QueryJobConfig()
        if dest_table_id is not None:
            table_id = f"{self.project_id}.{self.dataset_id}.{dest_table_id}"
            job_config.destination = table_id
            job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
            # set up write disposition for job
            job_config.write_disposition = {
                'WRITE_TRUNCATE': bigquery.WriteDisposition.WRITE_TRUNCATE,
                'WRITE_APPEND': bigquery.WriteDisposition.WRITE_APPEND,
                'WRITE_EMPTY': bigquery.WriteDisposition.WRITE_EMPTY,
            }[write_disposition]
            # relax schema restrictions
            if relaxed_schema and write_disposition == 'WRITE_APPEND':
                job_config.schema_update_options = [
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                ]
            # set up clustering fields
            if clustering_fields is not None:
                job_config.clustering_fields = clustering_fields
            # partition
            if partition_field:
                job_config.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field)  # name of column to use for partitioning
        else:
            table_id = "pandas DataFrame"
        # run query
        print(f"Running query to destination: {table_id}")
        query_job = self.client.query(
            query, location=self.location, job_config=job_config)
        query_job.result()
        if dest_table_id is None:
            return query_job.to_dataframe()
        print(f"Completed query to destination: {table_id}")
