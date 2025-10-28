from patterns.store.duckdb_storage import DuckDBStorage


def get_storage(storage_type: str = "duckdb", config: dict = None):
    """
    Returns a storage backend instance based on storage_type.
    Defaults to DuckDBStorage.
    `config` can be used to pass storage-specific params.
    """
    if storage_type == "duckdb":
        db_path = ":memory:"
        if config and "db_path" in config:
            db_path = config["db_path"]
        return DuckDBStorage(db_path=db_path)

    raise ValueError(f"Unknown storage_type: {storage_type}")
