"""AWS S3 implementation of PipelineStorage."""

import logging
import re
from collections.abc import Iterator
import urllib.parse
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError
from datashaper import Progress

from graphrag.index.progress import ProgressReporter

from .typing import PipelineStorage

log = logging.getLogger(__name__)


class S3PipelineStorage(PipelineStorage):
    """The S3 Storage implementation."""

    _aws_access_key_id: str | None
    _aws_secret_access_key: str | None
    _region_name: str | None
    _bucket_name: str
    _path_prefix: str
    _encoding: str

    def __init__(
        self,
        aws_access_key_id: str | None,
        aws_secret_access_key: str | None,
        region_name: str | None,
        bucket_name: str,
        encoding: str | None = None,
        path_prefix: str | None = None,
    ):
        """Create a new S3Storage instance."""
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._region_name = region_name
        self._bucket_name = bucket_name
        self._encoding = encoding or "utf-8"
        self._path_prefix = path_prefix or ""

        if aws_access_key_id and aws_secret_access_key:
            self._s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
        else:
            self._s3_client = boto3.client('s3')

        log.info(
            "creating S3 storage at bucket=%s, path=%s",
            self._bucket_name,
            self._path_prefix,
        )
        self.create_bucket()

    def create_bucket(self) -> None:
        """Create the bucket if it does not exist."""
        if not self.bucket_exists():
            try:
                if self._region_name:
                    self._s3_client.create_bucket(
                        Bucket=self._bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self._region_name}
                    )
                else:
                    self._s3_client.create_bucket(Bucket=self._bucket_name)
            except ClientError as e:
                log.error(f"Error creating bucket: {e}")
                raise

    def delete_bucket(self) -> None:
        """Delete the bucket."""
        if self.bucket_exists():
            try:
                self._s3_client.delete_bucket(Bucket=self._bucket_name)
            except ClientError as e:
                log.error(f"Error deleting bucket: {e}")
                raise

    def bucket_exists(self) -> bool:
        """Check if the bucket exists."""
        try:
            self._s3_client.head_bucket(Bucket=self._bucket_name)
            return True
        except ClientError:
            return False

    def find(
        self,
        file_pattern: re.Pattern[str],
        base_dir: str | None = None,
        progress: ProgressReporter | None = None,
        file_filter: dict[str, Any] | None = None,
        max_count=-1,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Find objects in a bucket using a file pattern, as well as a custom filter function."""
        base_dir = base_dir or ""

        log.info(
            "search bucket %s for files matching %s",
            self._bucket_name,
            file_pattern.pattern,
        )

        def objectname(object_name: str) -> str:
            if object_name.startswith(self._path_prefix):
                object_name = object_name.replace(self._path_prefix, "", 1)
            if object_name.startswith("/"):
                object_name = object_name[1:]
            return object_name

        def item_filter(item: dict[str, Any]) -> bool:
            if file_filter is None:
                return True
            return all(re.match(value, item[key]) for key, value in file_filter.items())

        try:
            paginator = self._s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self._bucket_name, Prefix=base_dir)

            num_loaded = 0
            num_filtered = 0
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    match = file_pattern.match(obj['Key'])
                    if match and obj['Key'].startswith(base_dir):
                        group = match.groupdict()
                        if item_filter(group):
                            yield (objectname(obj['Key']), group)
                            num_loaded += 1
                            if max_count > 0 and num_loaded >= max_count:
                                return
                        else:
                            num_filtered += 1
                    else:
                        num_filtered += 1
                    if progress is not None:
                        progress(
                            _create_progress_status(num_loaded, num_filtered, page['KeyCount'])
                        )
        except Exception:
            log.exception(
                "Error finding objects: base_dir=%s, file_pattern=%s, file_filter=%s",
                base_dir,
                file_pattern,
                file_filter,
            )
            raise

    async def get(
        self, key: str, as_bytes: bool | None = False, encoding: str | None = None
    ) -> Any:
        """Get a value from the storage."""
        try:
            # key = self._keyname(key)
            response = self._s3_client.get_object(Bucket=self._bucket_name, Key=key)
            data = response['Body'].read()
            # if not as_bytes:
            #     coding = encoding or "utf-8"
            #     data = data.decode(coding)
            return data
        except ClientError as e:
            print("Error getting key %s", e.response['Error']['Message'])
            return None
        else:
            return data

    async def set(self, key: str, value: Any, encoding: str | None = None) -> None:
        """Set a value in the storage."""
        try:
            key = self._keyname(key)
            if isinstance(value, bytes):
                self._s3_client.put_object(Bucket=self._bucket_name, Key=key, Body=value)
            else:
                coding = encoding or "utf-8"
                self._s3_client.put_object(Bucket=self._bucket_name, Key=key, Body=value.encode(coding))
        except ClientError:
            log.exception("Error setting key %s", key)

    def set_df_json(self, key: str, dataframe: Any) -> None:
        """Set a json dataframe."""
        buffer = dataframe.to_json(orient="records", lines=True, force_ascii=False)
        self._s3_client.put_object(Bucket=self._bucket_name, Key=self._keyname(key), Body=buffer)

    def set_df_parquet(self, key: str, dataframe: Any) -> None:
        """Set a parquet dataframe."""
        buffer = dataframe.to_parquet()
        self._s3_client.put_object(Bucket=self._bucket_name, Key=self._keyname(key), Body=buffer)

    async def has(self, key: str) -> bool:
        """Check if a key exists in the storage."""
        key = self._keyname(key)
        try:
            self._s3_client.head_object(Bucket=self._bucket_name, Key=key)
            return True
        except ClientError:
            return False

    async def delete(self, key: str) -> None:
        """Delete a key from the storage."""
        key = self._keyname(key)
        try:
            self._s3_client.delete_object(Bucket=self._bucket_name, Key=key)
        except ClientError:
            log.exception("Error deleting key %s", key)

    async def clear(self) -> None:
        """Clear the storage."""
        try:
            paginator = self._s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self._bucket_name):
                delete_keys = {'Objects': [{'Key': obj['Key']} for obj in page.get('Contents', [])]}
                if delete_keys['Objects']:
                    self._s3_client.delete_objects(Bucket=self._bucket_name, Delete=delete_keys)
        except ClientError:
            log.exception("Error clearing storage")

    def child(self, name: str | None) -> "PipelineStorage":
        """Create a child storage instance."""
        if name is None:
            return self
        path = str(Path(self._path_prefix) / name)
        return S3PipelineStorage(
            self._aws_access_key_id,
            self._aws_secret_access_key,
            self._region_name,
            self._bucket_name,
            self._encoding,
            path,
        )

    def _keyname(self, key: str) -> str:
        """Get the key name."""
        return str(Path(self._path_prefix) / key)


from typing import Optional, Dict, List, Any, Union
# def parse_query_string(query: Union[str, bytes, memoryview]) -> Dict[str, str]:
#     if isinstance(query, memoryview):
#         query = query.tobytes()
#     if isinstance(query, bytes):
#         query = query.decode('utf-8')
#     result = {}
#     for pair in query.split('&'):
#         if '=' in pair:
#             key, value = pair.split('=', 1)
#             result[key] = value
#     return result
def parse_query_string(query: Any) -> Dict[str, str]:
    # urllib.parse.parse_qsl can handle str, bytes, and memoryview
    parsed = urllib.parse.parse_qsl(query)
    return {k: v for k, v in parsed}

def create_blob_storage(
    connection_string: Optional[str],
    container_name: str,
    base_dir: str | None,
) -> PipelineStorage:
    """Create an S3 based storage."""
    log.info("Creating S3 storage at %s", container_name)
    if container_name is None:
        msg = "No bucket name provided for S3 storage."
        raise ValueError(msg)
    

    parsed_url = urllib.parse.urlparse(connection_string)
    
    # Manually parse the query string
    query_params = parse_query_string(parsed_url.query)
    
    endpoint_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    access_key_id = query_params.get('access_key_id')
    secret_access_key = query_params.get('secret_access_key')
    region_name = query_params.get('region')
    
    if not access_key_id or not secret_access_key:
        raise ValueError("Access key ID and Secret access key must be provided in the connection string")
    return S3PipelineStorage(
        access_key_id,
        secret_access_key,
        region_name,
        container_name,
        path_prefix=base_dir,
        # endpoint_url=endpoint_url,
    )


def validate_s3_bucket_name(bucket_name: str):
    """
    Check if the provided S3 bucket name is valid based on AWS rules.

    Args:
    -----
    bucket_name (str)
        The S3 bucket name to be validated.

    Returns
    -------
        bool: True if valid, False otherwise.
    """
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        return ValueError(
            f"Bucket name must be between 3 and 63 characters in length. Name provided was {len(bucket_name)} characters long."
        )

    if not bucket_name[0].isalnum():
        return ValueError(
            f"Bucket name must start with a letter or number. Starting character was {bucket_name[0]}."
        )

    if not re.match("^[a-z0-9.-]+$", bucket_name):
        return ValueError(
            f"Bucket name must only contain lowercase letters, numbers, periods, and hyphens. Name provided was {bucket_name}."
        )

    if ".." in bucket_name:
        return ValueError(
            f"Bucket name cannot contain two adjacent periods. Name provided was {bucket_name}."
        )

    if bucket_name.startswith("xn--"):
        return ValueError(
            f"Bucket name cannot start with 'xn--'. Name provided was {bucket_name}."
        )

    if bucket_name.endswith("-s3alias"):
        return ValueError(
            f"Bucket name cannot end with '-s3alias'. Name provided was {bucket_name}."
        )

    return True


def _create_progress_status(
    num_loaded: int, num_filtered: int, num_total: int
) -> Progress:
    return Progress(
        total_items=num_total,
        completed_items=num_loaded + num_filtered,
        description=f"{num_loaded} files loaded ({num_filtered} filtered)",
    )