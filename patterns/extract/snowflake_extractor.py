import os
import logging
from typing import List, Optional, Dict, Any

import snowflake.connector

from patterns.extract.base_extractor import BaseExtractor

logger = logging.getLogger("patterns")

class SnowflakeExtractor(BaseExtractor):
    @property
    def platform(self) -> str:
        return "snowflake"

    def _get_connection(self):
        """
        Get Snowflake connection with flexible authentication methods.
        
        Supports:
        1. SSO/Browser authentication (externalbrowser)
        2. Key-pair authentication (private_key_path)
        3. Password authentication
        4. Environment variables
        """
        params = self.connection.get("parameters", {})

        # Build connection parameters from config or environment variables
        conn_params = {
            'account': params.get('account') or os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': params.get('user') or os.getenv('SNOWFLAKE_USER'),
            'warehouse': params.get('warehouse') or os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': params.get('database') or os.getenv('SNOWFLAKE_DATABASE'),
            'schema': params.get('schema') or os.getenv('SNOWFLAKE_SCHEMA'),
            'role': params.get('role') or os.getenv('SNOWFLAKE_ROLE'),
        }

        # Remove None values
        conn_params = {k: v for k, v in conn_params.items() if v is not None}

        # Method 1: SSO/Browser authentication (recommended for development)
        if params.get('authenticator') == 'externalbrowser':
            logger.info("Using Snowflake SSO authentication (externalbrowser)")
            conn_params['authenticator'] = 'externalbrowser'

        # Method 2: Key-pair authentication (recommended for production)
        elif params.get('private_key_path') or os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'):
            private_key_path = params.get('private_key_path') or os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            private_key_passphrase = params.get('private_key_passphrase') or os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
            
            logger.info(f"Using Snowflake key-pair authentication from: {private_key_path}")
            
            try:
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.backends import default_backend
                
                with open(private_key_path, 'rb') as key_file:
                    if private_key_passphrase:
                        passphrase_bytes = private_key_passphrase.encode() if isinstance(private_key_passphrase, str) else private_key_passphrase
                    else:
                        passphrase_bytes = None
                    
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=passphrase_bytes,
                        backend=default_backend()
                    )
                
                pkb = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                conn_params['private_key'] = pkb
                
            except Exception as e:
                logger.error(f"Failed to load private key: {e}")
                raise

        # Method 3: Password authentication
        elif params.get('password') or os.getenv('SNOWFLAKE_PASSWORD'):
            logger.info("Using Snowflake password authentication")
            conn_params['password'] = params.get('password') or os.getenv('SNOWFLAKE_PASSWORD')

        # Validate required parameters
        if not conn_params.get('account') or not conn_params.get('user'):
            raise ValueError("Snowflake connection requires 'account' and 'user' parameters")

        if not any([
            conn_params.get('authenticator') == 'externalbrowser',
            conn_params.get('private_key'),
            conn_params.get('password')
        ]):
            raise ValueError("Snowflake connection requires one of: externalbrowser, private_key, or password")

        logger.info(f"Connecting to Snowflake account: {conn_params.get('account')}")
        return snowflake.connector.connect(**conn_params)

    def extract_tables(self) -> List[Dict[str, str]]:
        """
        List all tables across all databases and schemas with column information and size.
        Returns:
            List of dictionaries with 'database', 'schema', 'table', 'columns', 'size_bytes', etc.
        """
        tables = []
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW DATABASES")
                    databases = [row[1] for row in cur.fetchall()]

                    for db in databases:
                        try:
                            cur.execute(f"SHOW SCHEMAS IN DATABASE {db}")
                            schemas = [row[1] for row in cur.fetchall()]

                            for schema in schemas:
                                try:
                                    cur.execute(f"SHOW TABLES IN SCHEMA {db}.{schema}")
                                    table_rows = cur.fetchall()

                                    cur.execute(f"""
                                        SELECT table_name, column_name, data_type, is_nullable, column_default, comment
                                        FROM {db}.INFORMATION_SCHEMA.COLUMNS
                                        WHERE table_schema = '{schema}'
                                    """)
                                    column_rows = cur.fetchall()

                                    cur.execute(f"""
                                        WITH ranked_metrics AS (
                                          SELECT table_name,
                                                 active_bytes,
                                                 ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY table_created DESC) AS rn
                                          FROM {db}.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
                                          WHERE table_schema = '{schema}' AND NOT is_transient
                                        )
                                        SELECT table_name, active_bytes
                                        FROM ranked_metrics
                                        WHERE rn = 1;
                                    """)
                                    size_rows = cur.fetchall()
                                    size_map = {row[0]: row[1] for row in size_rows}

                                    from collections import defaultdict
                                    column_map = defaultdict(list)
                                    for row in column_rows:
                                        column_map[row[0]].append({
                                            "name": row[1],
                                            "type": row[2],
                                            "nullable": row[3],
                                            "default": row[4],
                                            "comment": row[5],
                                            "type_category": self.normalize_type_category(row[2]),
                                        })

                                    # Assemble table info with size
                                    for table_row in table_rows:
                                        table_name = table_row[1]

                                        tables.append({
                                            "database": db,
                                            "schema": schema,
                                            "table": table_name,
                                            "columns": column_map.get(table_name, []),
                                            "size_bytes": size_map.get(table_name),
                                            "extra": None,
                                            "source_platform": "snowflake",
                                            "source_project": conn.__getattribute__("account"),
                                            "source_region": None
                                        })

                                except Exception as e:
                                    logger.warning(f"Could not list tables, columns, or sizes in {db}.{schema}: {e}")
                        except Exception as e:
                            logger.warning(f"Could not list schemas in database {db}: {e}")

        except Exception as e:
            logger.error(f"Error listing tables, columns, and sizes: {e}")

        return tables

    def extract_query_history(self, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[
        Dict[str, Any]]:
        queries = []
        query = """
            SELECT 
                query_id,
                user_name,
                start_time,
                end_time,
                execution_status,
                query_text,
                query_type,
                bytes_scanned,
                total_elapsed_time,
                error_message
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE query_text IS NOT NULL
        """

        if start_time:
            query += f" AND start_time >= TO_TIMESTAMP('{start_time}')"
        if end_time:
            query += f" AND start_time <= TO_TIMESTAMP('{end_time}')"

        query += " ORDER BY start_time"

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    results = cur.fetchall()
                    for row in results:
                        queries.append({
                            "query_id": row[0],
                            "user_name": row[1],
                            "start_time": row[2].isoformat() if row[2] else None,
                            "end_time": row[3].isoformat() if row[3] else None,
                            "execution_status": row[4],
                            "query_text": row[5],
                            "statement_type": row[6],  # Snowflake's query_type
                            "bytes_scanned": row[7],
                            "execution_time_ms": row[8],
                            "error_message": row[9],
                            "extra": None, # Might leverage ACCESS_HISTORY table to get the exact list of objects accessed in the query
                            "source_platform": "snowflake",
                            "source_project": conn.__getattribute__("account"),
                            "source_region": None
                        })
        except Exception as e:
            logger.error(f"Error extracting Snowflake query history: {e}")

        return queries

    def get_type_map(self) -> dict:
        return {
            "text": {"STRING", "TEXT", "VARCHAR", "CHAR", "CHARACTER"},
            "integer": {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"},
            "float": {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"},
            "numeric": {"NUMBER", "NUMERIC", "DECIMAL"},
            "boolean": {"BOOLEAN"},
            "date": {"DATE"},
            "time": {"TIME"},
            "datetime": {"DATETIME"},
            "timestamp": {"TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"},
            "binary": {"BINARY", "VARBINARY"},
            "json": {"VARIANT", "OBJECT"},
            "array": {"ARRAY"},
            "geography": {"GEOGRAPHY"},
        }
