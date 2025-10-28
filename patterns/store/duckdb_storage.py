import os
import time
import duckdb
from typing import List, Dict, Any, Optional
import json
from patterns.store.base_storage import BaseStorage
import logging

logger = logging.getLogger("patterns")


class DuckDBStorage(BaseStorage):
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self.conn = duckdb.connect(database=db_path)
        self._init_tables_table()
        self._init_queries_table()
        self._init_read_table_queries_table()
        self._init_select_star_queries_table()
        self._init_partition_candidates_table()

    def _create_tables_table_sql(self) -> str:
        return """
            CREATE TABLE tables (
                database TEXT NOT NULL,
                schema TEXT NOT NULL,
                "table" TEXT NOT NULL,
                columns JSON,
                size_bytes BIGINT,
                extra JSON,
                source_platform TEXT NOT NULL,
                source_project TEXT NOT NULL,
                source_region TEXT,
                PRIMARY KEY (database, schema, "table", source_platform, source_project)
            );
        """

    def _init_tables_table(self):
        expected_columns = {
            "schema": "VARCHAR",
            "database": "VARCHAR",
            "table": "VARCHAR",
            "columns": "JSON",
            "size_bytes": "BIGINT",
            "extra": "JSON",
            "source_platform": "VARCHAR",
            "source_project": "VARCHAR",
            "source_region": "VARCHAR",
        }
        self._init_table("tables", self._create_tables_table_sql(), expected_columns)

    def save_tables(self, tables: List[Dict[str, Any]]) -> None:
        if not tables:
            logger.warning("No tables provided to save.")
            return

        source_platform = tables[0].get("source_platform")
        source_project = tables[0].get("source_project")

        if not source_platform or not source_project:
            logger.error("source_platform or source_project missing in table data; cannot save tables.")
            return

        logger.info(
            f"Deleting existing tables metadata for source_platform='{source_platform}', source_project='{source_project}'"
        )
        delete_sql = """
            DELETE FROM tables WHERE source_platform = ? AND source_project = ?;
        """
        self.conn.execute(delete_sql, (source_platform, source_project))

        insert_sql = """
            INSERT INTO tables (
                schema, database, "table",
                columns, size_bytes, extra,
                source_platform, source_project, source_region
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data_to_insert = [
            (
                t.get("schema"),
                t.get("database"),
                t.get("table"),
                json.dumps(t.get("columns")),
                t.get("size_bytes"),
                json.dumps(t.get("extra")),
                t.get("source_platform"),
                t.get("source_project"),
                t.get("source_region"),
            )
            for t in tables
        ]

        logger.info(
            f"Saving {len(data_to_insert)} tables metadata records for source '{source_platform}', project '{source_project}'"
        )
        self.conn.executemany(insert_sql, data_to_insert)

    def load_tables(self) -> List[Dict[str, Any]]:
        logger.info("Loading tables metadata from DuckDB")
        rows = self.conn.execute("SELECT * FROM tables").fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in rows]

    def _create_queries_table_sql(self) -> str:
        return """
            CREATE TABLE queries (
                query_id TEXT NOT NULL,
                user_name TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                execution_status TEXT,
                query_text TEXT NOT NULL,
                statement_type TEXT,
                bytes_scanned BIGINT,
                execution_time_ms BIGINT,
                error_message TEXT,
                extra JSON,
                source_platform TEXT NOT NULL,
                source_project TEXT,
                source_region TEXT,
                PRIMARY KEY (query_id, source_platform, source_project)
            );
        """

    def _init_queries_table(self):
        expected_columns = {
            "query_id": "VARCHAR",
            "user_name": "VARCHAR",
            "start_time": "TIMESTAMP",
            "end_time": "TIMESTAMP",
            "execution_status": "VARCHAR",
            "query_text": "VARCHAR",
            "statement_type": "VARCHAR",
            "bytes_scanned": "BIGINT",
            "execution_time_ms": "BIGINT",
            "error_message": "VARCHAR",
            "extra": "JSON",
            "source_platform": "VARCHAR",
            "source_project": "VARCHAR",
            "source_region": "VARCHAR",
        }
        self._init_table("queries", self._create_queries_table_sql(), expected_columns)

    def save_queries(self, queries: List[Dict[str, Any]]) -> None:
        if not queries:
            logger.warning("No queries provided to save.")
            return

        insert_sql = """
            INSERT INTO queries (
                query_id,
                user_name, start_time, end_time, execution_status, query_text,
                statement_type, bytes_scanned, execution_time_ms, error_message, extra,
                source_platform, source_project, source_region
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(query_id, source_platform, source_project) DO UPDATE SET
                user_name=excluded.user_name,
                start_time=excluded.start_time,
                end_time=excluded.end_time,
                execution_status=excluded.execution_status,
                query_text=excluded.query_text,
                statement_type=excluded.statement_type,
                bytes_scanned=excluded.bytes_scanned,
                execution_time_ms=excluded.execution_time_ms,
                error_message=excluded.error_message,
                extra=excluded.extra,
                source_region=excluded.source_region
        """

        data_to_insert = [
            (
                q.get("query_id"),
                q.get("user_name"),
                q.get("start_time"),
                q.get("end_time"),
                q.get("execution_status"),
                q.get("query_text"),
                q.get("statement_type"),
                q.get("bytes_scanned"),
                q.get("execution_time_ms"),
                q.get("error_message"),
                json.dumps(q.get("extra")),
                q.get("source_platform", ""),
                q.get("source_project"),
                q.get("source_region"),
            )
            for q in queries
        ]

        logger.info(f"Saving {len(data_to_insert)} query records.")
        self.conn.executemany(insert_sql, data_to_insert)

    def load_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        logger.info("Loading queries from DuckDB")

        base_query = "SELECT * FROM queries"

        base_query += f" WHERE source_platform = '{source_platform}' AND source_project = '{source_project}'"

        if start_time:
            base_query += f" AND start_time >= TIMESTAMP '{start_time}'"
        if end_time:
            base_query += f" AND start_time <= TIMESTAMP '{end_time}' "

        base_query += " ORDER BY start_time"

        rows = self.conn.execute(base_query).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in rows]

    # New methods for 'read_table_queries'
    def _create_read_table_queries_table_sql(self) -> str:
        return """
            CREATE TABLE read_table_queries (
                query_id TEXT NOT NULL,
                start_time TIMESTAMP,
                source_platform TEXT NOT NULL,
                source_project TEXT NOT NULL,
                database TEXT,
                schema TEXT,
                "table" TEXT,
                count INTEGER,
                error TEXT,
            );
        """

    def _init_read_table_queries_table(self):
        expected_columns = {
            "query_id": "VARCHAR",
            "start_time": "TIMESTAMP",
            "source_platform": "VARCHAR",
            "source_project": "VARCHAR",
            "database": "VARCHAR",
            "schema": "VARCHAR",
            "table": "VARCHAR",
            "count": "INTEGER",
            "error": "VARCHAR",
        }
        self._init_table("read_table_queries", self._create_read_table_queries_table_sql(), expected_columns)

    def save_read_table_queries(self, data: List[Dict[str, Any]]) -> None:
        if not data:
            logger.warning("No read_table_queries provided to save.")
            return

        source_platform = data[0].get("source_platform")
        source_project = data[0].get("source_project")

        if not source_platform or not source_project:
            logger.error("source_platform or source_project missing; cannot save read_table_queries.")
            return

        # Upsert logic: Delete old records before inserting new ones
        query_ids = [d.get("query_id") for d in data]
        if query_ids:
            delete_sql = f"DELETE FROM read_table_queries WHERE query_id IN ({','.join(['?'] * len(query_ids))}) AND source_platform = ? AND source_project = ?"
            self.conn.execute(delete_sql, query_ids + [source_platform, source_project])

        insert_sql = """
            INSERT INTO read_table_queries (
                query_id, start_time, source_platform, source_project, database, schema, "table", count, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data_to_insert = [
            (
                d.get("query_id"),
                d.get("start_time"),
                d.get("source_platform"),
                d.get("source_project"),
                d.get("database"),
                d.get("schema"),
                d.get("table"),
                d.get("count"),
                d.get("error"),
            )
            for d in data
        ]

        logger.info(f"Saving {len(data_to_insert)} read_table_queries records.")
        self.conn.executemany(insert_sql, data_to_insert)

    def load_read_table_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        logger.info("Loading read_table_queries from DuckDB")

        base_query = "SELECT * FROM read_table_queries"

        base_query += f" WHERE source_platform = '{source_platform}' AND source_project = '{source_project}'"

        if start_time:
            base_query += f" AND start_time >= TIMESTAMP '{start_time}'"
        if end_time:
            base_query += f" AND start_time <= TIMESTAMP '{end_time}' "

        base_query += " ORDER BY start_time"

        rows = self.conn.execute(base_query).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in rows]

    # New methods for 'select_star_queries'
    def _create_select_star_queries_table_sql(self) -> str:
        return """
            CREATE TABLE select_star_queries (
                query_id TEXT NOT NULL,
                source_platform TEXT NOT NULL,
                source_project TEXT NOT NULL,
                start_time TIMESTAMP,
                database TEXT,
                schema TEXT,
                "table" TEXT,
                count INTEGER,
                error TEXT,
            );
        """

    def _init_select_star_queries_table(self):
        expected_columns = {
            "query_id": "VARCHAR",
            "source_platform": "VARCHAR",
            "source_project": "VARCHAR",
            "start_time": "TIMESTAMP",
            "database": "VARCHAR",
            "schema": "VARCHAR",
            "table": "VARCHAR",
            "count": "INTEGER",
            "error": "VARCHAR",
        }
        self._init_table("select_star_queries", self._create_select_star_queries_table_sql(), expected_columns)

    def save_select_star_queries(self, data: List[Dict[str, Any]]) -> None:
        if not data:
            logger.warning("No select_star_queries provided to save.")
            return

        source_platform = data[0].get("source_platform")
        source_project = data[0].get("source_project")

        if not source_platform or not source_project:
            logger.error("source_platform or source_project missing; cannot save select_star_queries.")
            return

        # Upsert logic: Delete old records before inserting new ones
        query_ids = [d.get("query_id") for d in data]
        if query_ids:
            delete_sql = f"DELETE FROM select_star_queries WHERE query_id IN ({','.join(['?'] * len(query_ids))}) AND source_platform = ? AND source_project = ?"
            self.conn.execute(delete_sql, query_ids + [source_platform, source_project])

        insert_sql = """
            INSERT INTO select_star_queries (
                query_id, source_platform, source_project, start_time, database, schema, "table", count, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data_to_insert = [
            (
                d.get("query_id"),
                d.get("source_platform"),
                d.get("source_project"),
                d.get("start_time"),
                d.get("database"),
                d.get("schema"),
                d.get("table"),
                d.get("count"),
                d.get("error"),
            )
            for d in data
        ]

        logger.info(f"Saving {len(data_to_insert)} select_star_queries records.")
        self.conn.executemany(insert_sql, data_to_insert)

    def load_select_star_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        logger.info("Loading select_star_queries from DuckDB")

        base_query = "SELECT * FROM select_star_queries"

        base_query += f" WHERE source_platform = '{source_platform}' AND source_project = '{source_project}'"

        if start_time:
            base_query += f" AND start_time >= TIMESTAMP '{start_time}'"
        if end_time:
            base_query += f" AND start_time <= TIMESTAMP '{end_time}' "

        base_query += " ORDER BY start_time"

        rows = self.conn.execute(base_query).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in rows]

    # New methods for 'partition_candidates'
    def _create_partition_candidates_table_sql(self) -> str:
        return """
            CREATE TABLE partition_candidates (
                query_id TEXT NOT NULL,
                start_time TIMESTAMP,
                source_platform TEXT NOT NULL,
                source_project TEXT NOT NULL,
                database TEXT,
                schema TEXT,
                "table" TEXT,
                filter_type TEXT,
                "column" TEXT,
                count INTEGER,
                error TEXT,
            );
        """

    def _init_partition_candidates_table(self):
        expected_columns = {
            "query_id": "VARCHAR",
            "start_time": "TIMESTAMP",
            "source_platform": "VARCHAR",
            "source_project": "VARCHAR",
            "database": "VARCHAR",
            "schema": "VARCHAR",
            "table": "VARCHAR",
            "filter_type": "VARCHAR",
            "column": "VARCHAR",
            "count": "INTEGER",
            "error": "VARCHAR",
        }
        self._init_table("partition_candidates", self._create_partition_candidates_table_sql(), expected_columns)

    def save_partition_candidates(self, data: List[Dict[str, Any]]) -> None:
        if not data:
            logger.warning("No partition_candidates provided to save.")
            return

        source_platform = data[0].get("source_platform")
        source_project = data[0].get("source_project")

        if not source_platform or not source_project:
            logger.error("source_platform or source_project missing; cannot save partition_candidates.")
            return

        # Upsert logic: Delete old records before inserting new ones
        query_ids = [d.get("query_id") for d in data]
        if query_ids:
            delete_sql = f"DELETE FROM partition_candidates WHERE query_id IN ({','.join(['?'] * len(query_ids))}) AND source_platform = ? AND source_project = ?"
            self.conn.execute(delete_sql, query_ids + [source_platform, source_project])

        insert_sql = """
            INSERT INTO partition_candidates (
                query_id, start_time, source_platform, source_project, database, schema, "table", filter_type, "column", count, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data_to_insert = [
            (
                d.get("query_id"),
                d.get("start_time"),
                d.get("source_platform"),
                d.get("source_project"),
                d.get("database"),
                d.get("schema"),
                d.get("table"),
                d.get("filter_type"),
                d.get("column"),
                d.get("count"),
                d.get("error"),
            )
            for d in data
        ]

        logger.info(f"Saving {len(data_to_insert)} partition_candidates records.")
        self.conn.executemany(insert_sql, data_to_insert)

    def load_partition_candidates(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        logger.info("Loading partition_candidates from DuckDB")

        base_query = "SELECT * FROM partition_candidates"

        base_query += f" WHERE source_platform = '{source_platform}' AND source_project = '{source_project}'"

        if start_time:
            base_query += f" AND start_time >= TIMESTAMP '{start_time}'"
        if end_time:
            base_query += f" AND start_time <= TIMESTAMP '{end_time}' "

        base_query += " ORDER BY start_time"

        rows = self.conn.execute(base_query).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in rows]

    def _init_table(self, table_name: str, create_sql: str, expected_columns: Dict[str, str]):
        """General-purpose method to initialize a table, check schema, and handle mismatches."""

        def get_existing_schema():
            try:
                rows = self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                return {row[1]: row[2].upper() for row in rows}
            except Exception as e:
                logger.warning(f"Error checking existing schema for '{table_name}': {e}")
                return None

        existing_schema = get_existing_schema()
        if existing_schema:
            mismatch = False

            # 1. Check for expected columns missing or with wrong type
            for col, col_type in expected_columns.items():
                if col not in existing_schema or existing_schema[col] != col_type:
                    logger.warning(
                        f"Schema mismatch on column '{col}' in table '{table_name}': expected {col_type}, found {existing_schema.get(col)}"
                    )
                    mismatch = True

            # 2. Check for existing columns that are not in the expected schema
            for col, col_type in existing_schema.items():
                if col not in expected_columns:
                    logger.warning(
                        f"Unexpected column '{col}' found in table '{table_name}': it is not in the expected schema."
                    )
                    mismatch = True

            if mismatch:
                response = input(
                    f"The '{table_name}' table exists with an incompatible schema. Do you want to back it up and recreate it? [y/N]: "
                ).strip().lower()
                if response != "y":
                    logger.warning(f"Keeping existing table '{table_name}' with mismatched schema.")
                    return

                timestamp = time.strftime("%Y%m%d_%H%M%S")
                backup_name = f"{table_name}_backup_{timestamp}"
                logger.info(f"Backing up existing '{table_name}' table to '{backup_name}'")
                self.conn.execute(f"ALTER TABLE {table_name} RENAME TO {backup_name};")

                logger.info(f"Recreating '{table_name}' table with correct schema.")
                self.conn.execute(create_sql)
            else:
                logger.info(f"'{table_name}' table schema is correct.")
        else:
            logger.info(f"Creating '{table_name}' table for the first time.")
            self.conn.execute(create_sql)