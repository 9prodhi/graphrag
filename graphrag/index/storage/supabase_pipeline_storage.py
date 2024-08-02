import logging
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from supabase import create_client, Client
from datashaper import Progress

from graphrag.index.progress import ProgressReporter

from .typing import PipelineStorage

log = logging.getLogger(__name__)


class SupabasePipelineStorage(PipelineStorage):
    """The Supabase Storage implementation."""

    _supabase_url: str
    _supabase_key: str
    _bucket_name: str
    _path_prefix: str
    _encoding: str
    _client: Client

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        bucket_name: str,
        encoding: str | None = None,
        path_prefix: str | None = None,
    ):
        """Create a new SupabaseStorage instance."""
        self._supabase_url = supabase_url
        self._supabase_key = supabase_key
        self._client = create_client(self._supabase_url, self._supabase_key)
        self._encoding = encoding or "utf-8"
        self._bucket_name = bucket_name
        self._path_prefix = path_prefix or ""
        log.info(
            "creating Supabase storage at bucket=%s, path=%s",
            self._bucket_name,
            self._path_prefix,
        )
        self.create_bucket()

    def create_bucket(self) -> None:
        """Create the bucket if it does not exist."""
        if not self.bucket_exists():
            self._client.storage.create_bucket(self._bucket_name)

    def delete_bucket(self) -> None:
        """Delete the bucket."""
        if self.bucket_exists():
            self._client.storage.delete_bucket(self._bucket_name)

    def bucket_exists(self) -> bool:
        """Check if the bucket exists."""
        buckets = self._client.storage.list_buckets()
        return any(bucket.name == self._bucket_name for bucket in buckets)

    def find(
        self,
        file_pattern: re.Pattern[str],
        base_dir: str | None = None,
        progress: ProgressReporter | None = None,
        file_filter: dict[str, Any] | None = None,
        max_count=-1,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Find files in a bucket using a file pattern, as well as a custom filter function."""
        base_dir = base_dir or ""

        log.info(
            "search bucket %s for files matching %s",
            self._bucket_name,
            file_pattern.pattern,
        )

        def filename(file_path: str) -> str:
            if file_path.startswith(self._path_prefix):
                file_path = file_path.replace(self._path_prefix, "", 1)
            if file_path.startswith("/"):
                file_path = file_path[1:]
            return file_path

        def item_filter(item: dict[str, Any]) -> bool:
            if file_filter is None:
                return True
            return all(re.match(value, item[key]) for key, value in file_filter.items())

        try:
            all_files = self._client.storage.from_(self._bucket_name).list(base_dir)

            num_loaded = 0
            num_total = len(all_files)
            num_filtered = 0
            for file in all_files:
                match = file_pattern.match(file["name"])
                if match and file["name"].startswith(base_dir):
                    group = match.groupdict()
                    if item_filter(group):
                        yield (filename(file["name"]), group)
                        num_loaded += 1
                        if max_count > 0 and num_loaded >= max_count:
                            break
                    else:
                        num_filtered += 1
                else:
                    num_filtered += 1
                if progress is not None:
                    progress(
                        _create_progress_status(num_loaded, num_filtered, num_total)
                    )
        except Exception:
            log.exception(
                "Error finding files: base_dir=%s, file_pattern=%s, file_filter=%s",
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
            key = self._keyname(key)
            file_data = self._client.storage.from_(self._bucket_name).download(key)
            if not as_bytes:
                coding = encoding or "utf-8"
                file_data = file_data.decode(coding)
        except Exception:
            log.exception("Error getting key %s", key)
            return None
        else:
            return file_data

    async def set(self, key: str, value: Any, encoding: str | None = None) -> None:
        """Set a value in the storage."""
        try:
            key = self._keyname(key)
            if isinstance(value, bytes):
                file_data = value
            else:
                coding = encoding or "utf-8"
                file_data = value.encode(coding)
            self._client.storage.from_(self._bucket_name).upload(key, file_data)
        except Exception:
            log.exception("Error setting key %s", key)

    def set_df_json(self, key: str, dataframe: Any) -> None:
        """Set a json dataframe."""
        json_str = dataframe.to_json(orient="records", lines=True, force_ascii=False)
        self.set(key, json_str)

    def set_df_parquet(self, key: str, dataframe: Any) -> None:
        """Set a parquet dataframe."""
        parquet_bytes = dataframe.to_parquet()
        self.set(key, parquet_bytes)

    async def has(self, key: str) -> bool:
        """Check if a key exists in the storage."""
        key = self._keyname(key)
        try:
            self._client.storage.from_(self._bucket_name).download(key)
            return True
        except Exception:
            return False

    async def delete(self, key: str) -> None:
        """Delete a key from the storage."""
        key = self._keyname(key)
        self._client.storage.from_(self._bucket_name).remove([key])

    async def clear(self) -> None:
        """Clear the storage."""
        all_files = self._client.storage.from_(self._bucket_name).list()
        file_paths = [file["name"] for file in all_files]
        self._client.storage.from_(self._bucket_name).remove(file_paths)

    def child(self, name: str | None) -> "PipelineStorage":
        """Create a child storage instance."""
        if name is None:
            return self
        path = str(Path(self._path_prefix) / name)
        return SupabasePipelineStorage(
            self._supabase_url,
            self._supabase_key,
            self._bucket_name,
            self._encoding,
            path,
        )

    def _keyname(self, key: str) -> str:
        """Get the key name."""
        return str(Path(self._path_prefix) / key)


def create_supabase_storage(
    supabase_url: str,
    supabase_key: str,
    bucket_name: str,
    base_dir: str | None,
) -> PipelineStorage:
    """Create a Supabase based storage."""
    log.info("Creating Supabase storage at %s", bucket_name)
    if bucket_name is None:
        msg = "No bucket name provided for Supabase storage."
        raise ValueError(msg)
    return SupabasePipelineStorage(
        supabase_url,
        supabase_key,
        bucket_name,
        path_prefix=base_dir,
    )


def validate_supabase_bucket_name(bucket_name: str):
    """
    Check if the provided Supabase bucket name is valid.

    Args:
    -----
    bucket_name (str)
        The Supabase bucket name to be validated.

    Returns
    -------
        bool: True if valid, False otherwise.
    """
    # Supabase bucket naming rules are less restrictive than Azure's
    # Here we'll implement some basic checks
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        return ValueError(
            f"Bucket name must be between 3 and 63 characters in length. Name provided was {len(bucket_name)} characters long."
        )

    if not bucket_name.isalnum() and not all(c.isalnum() or c in '-_' for c in bucket_name):
        return ValueError(
            f"Bucket name must only contain alphanumeric characters, hyphens, or underscores. Name provided was {bucket_name}."
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