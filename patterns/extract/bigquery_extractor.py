import os
import logging
from datetime import timedelta
from typing import List, Optional, Dict, Any

from google.cloud import bigquery
from google.auth import default as google_auth_default
from google.oauth2 import service_account

from patterns.extract.base_extractor import BaseExtractor

logger = logging.getLogger("patterns")

class BigQueryExtractor(BaseExtractor):
    @property
    def platform(self) -> str:
        return "bigquery"

    def _get_client(self) -> bigquery.Client:
        """
        Get BigQuery client with flexible authentication methods.
        
        Supports:
        1. Service account key file path
        2. Service account credentials dict
        3. Application Default Credentials (ADC)
        4. Environment variables
        """
        params = self.connection.get("parameters", {})
        project_id = params.get("project_id") or params.get("project")

        # Method 1: Service account key file (recommended for production)
        if params.get("credentials_path"):
            credentials_path = params["credentials_path"]
            logger.info(f"Using BigQuery service account from file: {credentials_path}")
            
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
            
            client = bigquery.Client.from_service_account_json(
                credentials_path,
                project=project_id
            )
            return client

        # Method 2: Service account credentials dict (for backward compatibility)
        if params.get("private_key"):
            logger.info("Using BigQuery service account from credentials dict")
            credentials = service_account.Credentials.from_service_account_info(params)
            return bigquery.Client(credentials=credentials, project=project_id or params.get("project"))

        # Method 3: Environment variable GOOGLE_APPLICATION_CREDENTIALS
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            logger.info(f"Using BigQuery credentials from GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
            return bigquery.Client(project=project_id)

        # Method 4: Application Default Credentials (gcloud auth)
        logger.info("Using Application Default Credentials (ADC) for BigQuery")
        credentials, adc_project = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return bigquery.Client(credentials=credentials, project=project_id or adc_project)

    def extract_tables(self) -> List[Dict[str, Any]]:
        """
        List all tables across all datasets in the project, with column information and size.
        Returns:
            List of dicts with keys: 'database', 'schema', 'table', 'columns', 'size_bytes', etc.
        """
        from collections import defaultdict

        tables = []
        client = self._get_client()

        project_id = client.project or os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
        region = (
                self.connection.get("parameters", {}).get("region")
                or os.environ.get("GOOGLE_CLOUD_REGION")
                or "region-us"
        )

        try:
            datasets = list(client.list_datasets(project=project_id))
            if not datasets:
                logger.warning(f"No datasets found in project {project_id}")
                return tables

            for dataset in datasets:
                dataset_id = dataset.dataset_id
                # dataset_info = client.get_dataset(dataset_id)
                # dataset_location = dataset_info.location

                try:
                    tables_list = list(client.list_tables(dataset_id))
                    for table in tables_list:
                        table_id = table.table_id
                        try:
                            # Fetch full table metadata via API
                            table_ref = client.get_table(f"{project_id}.{dataset_id}.{table_id}")

                            columns = [
                                {
                                    "name": field.name,
                                    "type": field.field_type,
                                    "mode": field.mode,
                                    "description": field.description,
                                    "type_category": self.normalize_type_category(field.field_type),
                                }
                                for field in table_ref.schema
                            ]

                            tables.append({
                                "database": project_id,
                                "schema": dataset_id,
                                "table": table_id,
                                "columns": columns,
                                "size_bytes": table_ref.num_bytes,
                                "extra": None,
                                "source_platform": "bigquery",
                                "source_project": project_id,
                                "source_region": region,
                            })
                        except Exception as e:
                            logger.warning(
                                f"Could not fetch metadata for table {table_id} in dataset {dataset_id}: {e}")
                except Exception as e:
                    logger.warning(f"Could not process dataset {dataset_id}: {e}")

        except Exception as e:
            logger.error(f"Error listing datasets in project {project_id}: {e}")

        return tables

    def extract_query_history(self, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[
        Dict[str, Any]]:
        queries = []
        client = self._get_client()

        project_id = client.project or os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
        region = (
                self.connection.get("parameters", {}).get("region")
                or os.environ.get("GOOGLE_CLOUD_REGION")
                or "region-us"
        )

        base_query = f"""
            SELECT
                creation_time,
                job_id,
                user_email,
                query,
                statement_type,
                state,
                error_result,
                total_bytes_processed,
                total_slot_ms
            FROM `{project_id}.{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
            WHERE job_type = 'QUERY'
              AND creation_time IS NOT NULL
        """

        if start_time:
            base_query += f" AND creation_time >= TIMESTAMP('{start_time}')"
        if end_time:
            base_query += f" AND creation_time <= TIMESTAMP('{end_time}')"

        base_query += " ORDER BY creation_time"

        try:
            query_job = client.query(base_query)

            for row in query_job:
                creation_time = row.creation_time
                total_slot_ms = row.total_slot_ms or 0
                estimated_end_time = (
                    creation_time + timedelta(milliseconds=total_slot_ms)
                    if creation_time else None
                )

                queries.append({
                    "query_id": row.job_id,
                    "user_name": row.user_email,
                    "start_time": creation_time.isoformat() if creation_time else None,
                    "end_time": estimated_end_time.isoformat() if estimated_end_time else None,
                    "execution_status": row.state,
                    "query_text": row.query,
                    "statement_type": row.statement_type,
                    "bytes_scanned": row.total_bytes_processed,
                    "execution_time_ms": total_slot_ms,
                    "error_message": row.error_result.get("message") if row.error_result else None,
                    "extra": None,
                    "source_platform": "bigquery",
                    "source_project": project_id,
                    "source_region": region,
                })

        except Exception as e:
            logger.error(f"Error extracting BigQuery query history: {e}")

        return queries

    def get_type_map(self) -> dict:
        return {
            "text": {"STRING"},
            "integer": {"INT", "INT64", "INTEGER"},
            "float": {"FLOAT", "FLOAT32", "FLOAT64", "DOUBLE"},
            "numeric": {"NUMERIC", "BIGNUMERIC"},
            "boolean": {"BOOLEAN", "BOOL"},
            "date": {"DATE"},
            "time": {"TIME"},
            "datetime": {"DATETIME"},
            "timestamp": {"TIMESTAMP"},
            "binary": {"BYTES"},
            "json": {"JSON", "STRUCT"},
            "array": {"ARRAY", "REPEATED"},
            "geography": {"GEOGRAPHY"},
        }
