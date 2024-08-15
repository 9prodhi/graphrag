# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Command line interface for the query module."""

# import os
# from pathlib import Path
# from typing import cast, Union
# import re

# import pandas as pd

# from graphrag.config import (
#     GraphRagConfig,
#     create_graphrag_config,
# )
# from graphrag.index.progress import PrintProgressReporter
# from graphrag.model.entity import Entity
# from graphrag.query.input.loaders.dfs import (
#     store_entity_semantic_embeddings,
# )
# from graphrag.vector_stores import VectorStoreFactory, VectorStoreType
# from graphrag.vector_stores.lancedb import LanceDBVectorStore

# from .factories import get_global_search_engine, get_local_search_engine
# from .indexer_adapters import (
#     read_indexer_covariates,
#     read_indexer_entities,
#     read_indexer_relationships,
#     read_indexer_reports,
#     read_indexer_text_units,
# )

# from graphrag.index.storage import DataPath

# reporter = PrintProgressReporter("")
# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Command line interface for the query module."""

# Standard library imports
import os
import re
import asyncio
from pathlib import Path
from typing import cast, Union, List, Tuple, Dict, Any
from collections import defaultdict
from datetime import datetime
from io import BytesIO

# Third-party imports
import pandas as pd
import yaml
import json

# GraphRag imports
from graphrag.config import (
    GraphRagConfig,
    create_graphrag_config,
)
from graphrag.config.enums import (
    CacheType,
    InputFileType,
    ReportingType,
    StorageType,
    TextEmbeddingTarget,
)
from graphrag.index.progress import PrintProgressReporter
from graphrag.index.storage import DataPath, PipelineStorage, load_storage
from graphrag.index.config.storage import (
    PipelineStorageConfigTypes,
    PipelineStorageConfig,
    PipelineFileStorageConfig,
    PipelineMemoryStorageConfig,
    PipelineBlobStorageConfig,
)
from graphrag.model.entity import Entity
from graphrag.query.input.loaders.dfs import store_entity_semantic_embeddings
from graphrag.vector_stores import VectorStoreFactory, VectorStoreType
from graphrag.vector_stores.lancedb import LanceDBVectorStore

# Local imports
from .factories import get_global_search_engine, get_local_search_engine
from .indexer_adapters import (
    read_indexer_covariates,
    read_indexer_entities,
    read_indexer_relationships,
    read_indexer_reports,
    read_indexer_text_units,
)

# Initialize reporter
reporter = PrintProgressReporter("")

# Rest of the code remains the same...

def __get_embedding_description_store(
    entities: list[Entity],
    vector_store_type: str = VectorStoreType.LanceDB,
    config_args: dict | None = None,
):
    """Get the embedding description store."""
    if not config_args:
        config_args = {}

    collection_name = config_args.get(
        "query_collection_name", "entity_description_embeddings"
    )
    config_args.update({"collection_name": collection_name})
    description_embedding_store = VectorStoreFactory.get_vector_store(
        vector_store_type=vector_store_type, kwargs=config_args
    )

    description_embedding_store.connect(**config_args)

    if config_args.get("overwrite", False):
        # this step assumps the embeddings where originally stored in a file rather
        # than a vector database

        # dump embeddings from the entities list to the description_embedding_store
        store_entity_semantic_embeddings(
            entities=entities, vectorstore=description_embedding_store
        )
    else:
        # load description embeddings to an in-memory lancedb vectorstore
        # to connect to a remote db, specify url and port values.
        description_embedding_store = LanceDBVectorStore(
            collection_name=collection_name
        )
        description_embedding_store.connect(
            db_uri=config_args.get("db_uri", "./lancedb")
        )

        # load data from an existing table
        description_embedding_store.document_collection = (
            description_embedding_store.db_connection.open_table(
                description_embedding_store.collection_name
            )
        )

    return description_embedding_store




def run_global_search(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    query: str,
):
    """Run a global search with the given query."""
    data_path, root_dir, config = _configure_paths_and_settings(
        data_dir, root_dir, config_dir
    )


    final_nodes: pd.DataFrame = read_parquet_from_storage(
        data_path, "create_final_nodes.parquet", config
    )
    final_entities: pd.DataFrame = read_parquet_from_storage(
        data_path , "create_final_entities.parquet", config
    )
    final_community_reports: pd.DataFrame = read_parquet_from_storage(
        data_path , "create_final_community_reports.parquet", config
    )


    reports = read_indexer_reports(
        final_community_reports, final_nodes, community_level
    )
    entities = read_indexer_entities(final_nodes, final_entities, community_level)
    search_engine = get_global_search_engine(
        config,
        reports=reports,
        entities=entities,
        response_type=response_type,
    )

    result = search_engine.search(query=query)

    reporter.success(f"Global Search Response: {result.response}")
    return result.response


def run_local_search(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    query: str,
):
    """Run a local search with the given query."""
    data_path, root_dir, config = _configure_paths_and_settings(
        data_dir, root_dir, config_dir
    )
    # Just to fix the errors, need code changes for the local search as well


    final_nodes: pd.DataFrame = read_parquet_from_storage(
    data_path, "create_final_nodes.parquet", config
    )

    final_community_reports =read_parquet_from_storage(
        data_path , "create_final_community_reports.parquet", config
    )
    final_text_units = read_parquet_from_storage(data_path , "create_final_text_units.parquet", config)
    final_relationships = read_parquet_from_storage(
        data_path, "create_final_relationships.parquet", config
    )
    final_entities = read_parquet_from_storage(data_path, "create_final_entities.parquet", config)
    # final_covariates_path = data_path +"/create_final_covariates.parquet"
    # final_covariates = (
    #     pd.read_parquet(final_covariates_path)
    #     if final_covariates_path.exists()
    #     else None
    # )
    final_covariates = None

    vector_store_args = (
        config.embeddings.vector_store if config.embeddings.vector_store else {}
    )

    reporter.info(f"Vector Store Args: {vector_store_args}")
    vector_store_type = vector_store_args.get("type", VectorStoreType.LanceDB)

    entities = read_indexer_entities(final_nodes, final_entities, community_level)
    description_embedding_store = __get_embedding_description_store(
        entities=entities,
        vector_store_type=vector_store_type,
        config_args=vector_store_args,
    )
    covariates = (
        read_indexer_covariates(final_covariates)
        if final_covariates is not None
        else []
    )

    search_engine = get_local_search_engine(
        config,
        reports=read_indexer_reports(
            final_community_reports, final_nodes, community_level
        ),
        text_units=read_indexer_text_units(final_text_units),
        entities=entities,
        relationships=read_indexer_relationships(final_relationships),
        covariates={"claims": covariates},
        description_embedding_store=description_embedding_store,
        response_type=response_type,
    )

    result = search_engine.search(query=query)
    reporter.success(f"Local Search Response: {result.response}")
    return result.response



def _configure_paths_and_settings(
    data_dir: str | None,
    root_dir: str | None,
    config_dir: str | None,
) -> tuple[str, str | None, GraphRagConfig]:
    if data_dir is None and root_dir is None:
        msg = "Either data_dir or root_dir must be provided."
        raise ValueError(msg)

    config = _create_graphrag_config(root_dir, config_dir)
    if data_dir is None:
        data_path =  _infer_data_dir(cast(str, root_dir), config)

    
    return data_path, root_dir, config


def _infer_data_dir(root: str, config: GraphRagConfig) -> str:
    storage_config = config.storage
    if not isinstance(storage_config, PipelineStorageConfig):
        # Convert StorageConfig to appropriate PipelineStorageConfig subclass
        if storage_config.type == StorageType.blob:
            storage_config = PipelineBlobStorageConfig(**storage_config.model_dump())
        else:
            raise ValueError(f"Unsupported storage type: {storage_config.type}")

    if storage_config.type == StorageType.blob:
        # For blob storage (including S3 and Supabase)
        storage = load_storage(storage_config)
        return _infer_blob_data_dir(storage)
    else:
        # For local file system
        return _infer_local_data_dir(root)


def _infer_blob_data_dir(storage: PipelineStorage) -> str:
    """Infer the data directory in blob storage (S3)."""
    output_prefix = "output/"
    artifacts_suffix = "artifacts/"
    
    all_objects: List[Tuple[str, Dict[str, Any]]] = list(storage.find(
        re.compile(f"^{output_prefix}")
    ))
    
    if not all_objects:
        raise ValueError(f"No objects found in {output_prefix}")
    
    # Group objects by their date directory
    date_dirs = defaultdict(list)
    for obj in all_objects:
        key = obj[0]
        match = re.match(f"^{output_prefix}(\\d{{8}}-\\d{{6}})/", key)
        if match:
            date_dir = match.group(1)
            date_dirs[date_dir].append(obj)
    
    if not date_dirs:
        raise ValueError(f"No valid date directories found in {output_prefix}")
    
    # Find the latest date directory that contains 'artifacts/'
    latest_dir = None
    latest_date = None
    for date_dir, objects in date_dirs.items():
        if any(f"{output_prefix}{date_dir}/{artifacts_suffix}" in obj[0] for obj in objects):
            dir_date = datetime.strptime(date_dir, "%Y%m%d-%H%M%S")
            if latest_date is None or dir_date > latest_date:
                latest_date = dir_date
                latest_dir = date_dir
    
    if latest_dir is None:
        raise ValueError(f"No artifacts directory found in any date directory under {output_prefix}")
    
    # Construct the path to the artifacts folder
    artifacts_path = f"{output_prefix}{latest_dir}/{artifacts_suffix}"
    
    return artifacts_path

def _infer_local_data_dir(root: str) -> str:
    output = Path(root) / "output"
    if output.exists():
        folders = sorted(output.iterdir(), key=os.path.getmtime, reverse=True)
        if folders:
            folder = folders[0]
            return str((folder / "artifacts").absolute())
    raise ValueError(f"Could not infer data directory from root={root}")


def read_parquet_from_storage(data_path: str, file_name: str, config: GraphRagConfig) -> pd.DataFrame:
    """Read a parquet file from either local storage or blob storage."""
    if config.storage.type == StorageType.file:
        # Local file system
        return pd.read_parquet(Path(data_path) / file_name)

    elif config.storage.type == StorageType.blob:
        if not isinstance(config.storage, PipelineBlobStorageConfig):
            storage_config = PipelineBlobStorageConfig(**config.storage.model_dump())
        else:
            storage_config = config.storage

        # Create a PipelineStorage instance
        # Note: try dict or manual type conversion
        storage = load_storage(storage_config)

        file_path = f"{data_path.rstrip('/')}/{file_name}"
        matching_objects = list(storage.find(re.compile(re.escape(file_path))))

        if matching_objects:
            print(f"Found matching objects: {matching_objects}")
        else:
            print(f"No matching objects found for {file_path}")
        try:
            file_content = asyncio.run(storage.get(file_path))
            if file_content is None:
                raise FileNotFoundError(f"File {file_name} not found in storage at {data_path}")
            print(f"Successfully retrieved file content for {file_path}")
            return pd.read_parquet(BytesIO(file_content))
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            raise
    else:
        raise ValueError(f"Unsupported  type: {type(config.storage)}")



def _create_graphrag_config(
    root: str | None,
    config_dir: str | None,
) -> GraphRagConfig:
    """Create a GraphRag configuration."""
    return _read_config_parameters(root or "./", config_dir)


def _read_config_parameters(root: str, config: str | None):
    _root = Path(root)
    settings_yaml = (
        Path(config)
        if config and Path(config).suffix in [".yaml", ".yml"]
        else _root / "settings.yaml"
    )
    if not settings_yaml.exists():
        settings_yaml = _root / "settings.yml"

    if settings_yaml.exists():
        reporter.info(f"Reading settings from {settings_yaml}")
        with settings_yaml.open(
            "rb",
        ) as file:
            import yaml

            data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
            return create_graphrag_config(data, root)

    settings_json = (
        Path(config)
        if config and Path(config).suffix == ".json"
        else _root / "settings.json"
    )
    if settings_json.exists():
        reporter.info(f"Reading settings from {settings_json}")
        with settings_json.open("rb") as file:
            import json

            data = json.loads(file.read().decode(encoding="utf-8", errors="strict"))
            return create_graphrag_config(data, root)

    reporter.info("Reading settings from environment variables")
    return create_graphrag_config(root_dir=root)
