from pathlib import Path
import asyncio
from typing import Union
import pandas as pd
from io import BytesIO
from .typing import PipelineStorage

class DataPath:
    def __init__(self, path_or_storage: Union[str, PipelineStorage], is_supabase: bool = False):
        self.path_or_storage = path_or_storage
        self.is_supabase = is_supabase
        self._path = "" if is_supabase else str(path_or_storage)

    def join(self, *parts: str) -> 'DataPath':
        if self.is_supabase:
            new_path = '/'.join([self._path] + list(parts))
            new_instance = DataPath(self.path_or_storage, is_supabase=True)
            new_instance._path = new_path
            return new_instance
        else:
            new_path = str(Path(self._path).joinpath(*parts))
            return DataPath(new_path, is_supabase=False)

    def read_parquet(self) -> pd.DataFrame:
        if self.is_supabase:
            if not isinstance(self.path_or_storage, PipelineStorage):
                raise TypeError("Supabase storage must be a PipelineStorage instance")
            content =  asyncio.run(self.path_or_storage.get(self._path, as_bytes=True))
            return pd.read_parquet(BytesIO(content))
        else:
            return pd.read_parquet(self._path)

    def __str__(self) -> str:
        return self._path

    @property
    def path(self) -> str:
        return self._path