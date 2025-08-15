import io
import os
import logging
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowIOError
from src.loader_process.loaders.base_loader import BaseLoader
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import DatabaseError


class ClickHouseLoader(BaseLoader):
    """
    Configurable ClickHouse loader implementing high-throughput insertion with deduplication.

    This loader works with any data source by accepting configurable table names and column schemas.

    Args:
        append_only_table: Name of the append-only table for initial data insertion
        rmt_table: Name of the ReplacingMergeTree table for deduplicated data
        temp_table_template: Base name for temporary tables (unique timestamp will be appended)
        columns: List of column names, with last column assumed to be 'created_at'

    Architecture:
        Temp Table -> Append Only Table -> (Materialized View) -> ReplacingMergeTree Table

    Key Features:
        - High throughput insertions using temporary tables
        - Automatic deduplication via ReplacingMergeTree with materialized views
        - Automatic cleanup of temporary resources
        - Comprehensive error handling and logging
        - Configurable for any data source

    Example Usage:
        loader = ClickHouseLoader(
            append_only_table="crypto_prices_append_only",
            rmt_table="crypto_prices_rmt",
            temp_table_template="crypto_prices_temp",
            columns=["instrument", "timestamp", "prices", "created_at"]
        )
        loader.load(parquet_data)

    Example DDL Setup:
        ```sql
        CREATE TABLE {temp_table_template}(
            -- Your columns here based on data source
        ) Engine=MergeTree ORDER BY (primary_key_columns);

        CREATE TABLE {append_only_table} LIKE {temp_table_template}
        PARTITION BY YYYYMM(timestamp_column)
        ORDER BY (primary_key_columns);

        CREATE TABLE {rmt_table} LIKE {append_only_table}
        Engine=ReplacingMergeTree(created_at)
        PARTITION BY YYYYMM(timestamp_column)
        ORDER BY (primary_key_columns);

        CREATE MATERIALIZED VIEW {data_source}_mv AS
        INSERT INTO {rmt_table} SELECT * FROM {append_only_table};
        ```
    """

    def __init__(
        self,
        append_only_table: str,
        rmt_table: str,
        temp_table_template: str,
        columns: list[str],
    ) -> None:
        try:
            self._client = create_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                username=os.getenv("CLICKHOUSE_USERNAME", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("CLICKHOUSE_DATABASE", "default"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            )
        except DatabaseError as e:
            logging.error(f"Failed to connect to ClickHouse: {e}")
            raise

        self._append_only_table = append_only_table
        self._rmt_table = rmt_table
        self._temp_table_template = temp_table_template
        self._columns = columns
        self._temp_table: str | None = None

    def _sync_all_clusters_replicas(self) -> None:
        # Skip cluster sync for single-node ClickHouse instances
        # try:
        #     # sync metadata on what partitions exists across all server nodes
        #     self._client.raw_query("SYSTEM SYNC REPLICA")
        # except DatabaseError as e:
        #     logging.error(f"Failed to sync cluster replicas: {e}")
        #     raise
        pass

    def _create_temporary_table(self) -> None:
        datetime_str: str = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        self._temp_table = f"{self._temp_table_template}_{datetime_str}"
        try:
            self._client.raw_query(
                f"CREATE TABLE {self._temp_table} AS {self._temp_table_template}"
            )
            logging.info(f"Created temporary table: {self._temp_table}")
        except DatabaseError as e:
            logging.error(f"Failed to create temporary table {self._temp_table}: {e}")
            raise

    def _deserialize_parquet(self, data: io.BytesIO) -> pa.Table:
        """
        Deserialize binary parquet data into a PyArrow Table
        """
        try:
            # Reset buffer position to beginning
            data.seek(0)
            table = pq.read_table(data)
            logging.info(
                f"Deserialized parquet data: {table.num_rows} rows, {table.num_columns} columns"
            )
            return table
        except (ArrowInvalid, ArrowIOError, OSError) as e:
            logging.error(f"Failed to deserialize parquet data: {e}")
            raise ValueError(f"Invalid parquet data: {e}") from e

    def _copy_to_temporary_table(self, data: io.BytesIO) -> None:
        """
        io.BytesIO is a binary encoded pyarrow.parquet file
        """
        if self._temp_table is None:
            raise ValueError("Temporary table not created")

        try:
            # Deserialize parquet data to PyArrow Table
            table = self._deserialize_parquet(data)

            # Insert PyArrow Table with schema validation and type safety
            # Explicitly set async_insert=0 for synchronous inserts
            self._client.insert_arrow(
                table=self._temp_table, arrow_table=table, settings={"async_insert": 0}
            )
            logging.info(f"Copied data to temporary table: {self._temp_table}")
        except DatabaseError as e:
            logging.error(
                f"Failed to copy data to temporary table {self._temp_table}: {e}"
            )
            raise

    def _insert_from_temporary_table_to_append_only_table(self) -> None:
        if self._temp_table is None:
            raise ValueError("Temporary table not created")

        try:
            # Explicitly set async_insert=0 for synchronous inserts
            columns_str = ", ".join(self._columns)
            select_columns = ", ".join(self._columns[:-1]) + ", now() as created_at"

            self._client.raw_query(
                f"""
                INSERT INTO {self._append_only_table}({columns_str}) 
                SELECT {select_columns} FROM {self._temp_table}
                """,
                settings={"async_insert": 0},
            )
            logging.info(
                f"Inserted data from {self._temp_table} to {self._append_only_table}"
            )
        except DatabaseError as e:
            logging.error(
                f"Failed to insert from {self._temp_table} to {self._append_only_table}: {e}"
            )
            raise

    def _cleanup_temporary_table(self) -> None:
        if self._temp_table is None:
            return

        try:
            self._client.raw_query(f"DROP TABLE IF EXISTS {self._temp_table}")
            logging.info(f"Cleaned up temporary table: {self._temp_table}")
        except DatabaseError as e:
            logging.warning(
                f"Failed to cleanup temporary table {self._temp_table}: {e}"
            )
        finally:
            self._temp_table = None

    def load(self, data: io.BytesIO) -> None:
        try:
            self._create_temporary_table()
            self._copy_to_temporary_table(data)
            self._sync_all_clusters_replicas()
            self._insert_from_temporary_table_to_append_only_table()
        finally:
            self._cleanup_temporary_table()
