import io
import os
import pytest
import logging
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import DatabaseError
import time
from src.loader_process.loaders.clickhouse_loader import ClickHouseLoader


@pytest.fixture
def clickhouse_client():
    """Create ClickHouse client for testing"""
    client = create_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        username=os.getenv("CLICKHOUSE_USERNAME", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "password"),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    )
    return client


@pytest.fixture
def test_table_names():
    """Generate unique table names for testing"""
    return {
        "temp_template": "test_crypto_prices_temp",
        "append_only": "test_crypto_prices_append_only",
        "rmt": "test_crypto_prices_rmt",
        "mv": "test_crypto_prices_mv",
    }


@pytest.fixture
def setup_test_tables(clickhouse_client, test_table_names):
    """Setup test tables and materialized view"""
    client = clickhouse_client
    tables = test_table_names

    # DDL for temporary table template
    temp_table_ddl = f"""
    CREATE TABLE {tables['temp_template']}(
        instrument String CODEC(ZSTD(1)),
        timestamp DateTime(9, 'UTC') CODEC(Delta, ZSTD(1)),
        prices Float64 CODEC(ZSTD(1)),
        created_at DateTime(9, 'UTC') CODEC(Delta, ZSTD(1))
    ) Engine=MergeTree
    ORDER BY (instrument, timestamp)
    """

    # DDL for append-only table
    append_only_ddl = f"""
    CREATE TABLE {tables['append_only']}(
        instrument String CODEC(ZSTD(1)),
        timestamp DateTime(9, 'UTC') CODEC(Delta, ZSTD(1)),
        prices Float64 CODEC(ZSTD(1)),
        created_at DateTime(9, 'UTC') CODEC(Delta, ZSTD(1))
    ) Engine=MergeTree
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (instrument, timestamp)
    """

    # DDL for ReplacingMergeTree table
    rmt_ddl = f"""
    CREATE TABLE {tables['rmt']}(
        instrument String CODEC(ZSTD(1)),
        timestamp DateTime(9, 'UTC') CODEC(Delta, ZSTD(1)),
        prices Float64 CODEC(ZSTD(1)),
        created_at DateTime(9, 'UTC') CODEC(Delta, ZSTD(1))
    ) Engine=ReplacingMergeTree(created_at)
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (instrument, timestamp)
    """

    # DDL for materialized view
    mv_ddl = f"""
    CREATE MATERIALIZED VIEW {tables['mv']} TO {tables['rmt']} AS
    SELECT * FROM {tables['append_only']}
    """

    try:
        # Create tables
        client.raw_query(temp_table_ddl)
        client.raw_query(append_only_ddl)
        client.raw_query(rmt_ddl)
        client.raw_query(mv_ddl)

        logging.info(f"Created test tables: {tables}")
        yield tables

    finally:
        # Cleanup tables
        cleanup_queries = [
            f"DROP VIEW IF EXISTS {tables['mv']}",
            f"DROP TABLE IF EXISTS {tables['rmt']}",
            f"DROP TABLE IF EXISTS {tables['append_only']}",
            f"DROP TABLE IF EXISTS {tables['temp_template']}",
        ]

        for query in cleanup_queries:
            try:
                client.raw_query(query)
            except DatabaseError as e:
                logging.warning(f"Failed to cleanup: {query}, error: {e}")


@pytest.fixture
def sample_crypto_data():
    """Generate sample crypto prices data as PyArrow Table"""
    base_time = datetime(2025, 8, 15, 10, 0, 0, tzinfo=timezone.utc)

    # Sample data
    instruments = [
        "BTCUSD",
        "ETHUSD",
        "BTCUSD",
        "ETHUSD",
    ]  # Duplicate BTCUSD to test deduplication
    timestamps = [
        base_time,
        base_time,
        base_time,  # Same timestamp as first BTCUSD - should be deduplicated
        base_time.replace(minute=1),
    ]
    prices = [50000.0, 3000.0, 50001.0, 3001.0]  # Different price for duplicate

    # Create PyArrow Table
    table = pa.table(
        {
            "instrument": pa.array(instruments, type=pa.string()),
            "timestamp": pa.array(timestamps, type=pa.timestamp("ns", tz="UTC")),
            "prices": pa.array(prices, type=pa.float64()),
        }
    )

    return table


@pytest.fixture
def parquet_data(sample_crypto_data):
    """Convert sample data to binary parquet format"""
    buffer = io.BytesIO()
    pq.write_table(sample_crypto_data, buffer)
    buffer.seek(0)
    return buffer


def test_clickhouse_loader_integration(
    clickhouse_client, setup_test_tables, parquet_data
):
    """Integration test for ClickHouseLoader"""
    client = clickhouse_client
    tables = setup_test_tables

    # Initialize loader
    loader = ClickHouseLoader(
        append_only_table=tables["append_only"],
        rmt_table=tables["rmt"],
        temp_table_template=tables["temp_template"],
        columns=["instrument", "timestamp", "prices", "created_at"],
    )

    # Load data
    loader.load(parquet_data)

    # Wait a moment for materialized view to process

    time.sleep(1)

    # Verify data in append-only table
    append_only_result = client.query(
        f"SELECT instrument, timestamp, prices FROM {tables['append_only']} ORDER BY instrument, timestamp"
    )
    assert (
        len(append_only_result.result_rows) == 4
    ), "Should have 4 rows in append-only table"

    # Verify data in ReplacingMergeTree table (with FINAL for deduplication)
    rmt_result = client.query(
        f"""
        SELECT instrument, timestamp, prices 
        FROM {tables['rmt']} FINAL 
        ORDER BY instrument, timestamp
        """
    )

    # Should have 3 unique rows after deduplication (BTCUSD at same timestamp should be deduplicated)
    rows = rmt_result.result_rows
    assert len(rows) == 3, f"Expected 3 rows after deduplication, got {len(rows)}"

    # Verify specific data
    btc_rows = [row for row in rows if row[0] == "BTCUSD"]
    eth_rows = [row for row in rows if row[0] == "ETHUSD"]

    assert len(btc_rows) == 1, "Should have 1 BTCUSD row after deduplication"
    assert len(eth_rows) == 2, "Should have 2 ETHUSD rows"

    # Verify the deduplicated BTCUSD has the latest price (50001.0)
    btc_price = btc_rows[0][2]
    assert (
        btc_price == 50001.0
    ), f"Expected deduplicated BTCUSD price to be 50001.0, got {btc_price}"

    logging.info(
        f"Integration test passed: {len(rows)} rows in RMT table after deduplication"
    )


def test_loader_error_handling(setup_test_tables):
    """Test error handling with invalid data"""
    tables = setup_test_tables

    loader = ClickHouseLoader(
        append_only_table=tables["append_only"],
        rmt_table=tables["rmt"],
        temp_table_template=tables["temp_template"],
        columns=["instrument", "timestamp", "prices", "created_at"],
    )

    # Test with invalid parquet data
    invalid_data = io.BytesIO(b"not a parquet file")

    with pytest.raises(ValueError, match="Invalid parquet data"):
        loader.load(invalid_data)


def test_loader_schema_validation(setup_test_tables, clickhouse_client):
    """Test schema validation with mismatched columns"""
    tables = setup_test_tables
    client = clickhouse_client

    # Create table with different schema
    wrong_schema_table = f"{tables['temp_template']}_wrong"
    client.raw_query(
        f"""
        CREATE TABLE {wrong_schema_table}(
            different_column String
        ) Engine=MergeTree
        ORDER BY different_column
    """
    )

    try:
        loader = ClickHouseLoader(
            append_only_table=tables["append_only"],
            rmt_table=tables["rmt"],
            temp_table_template=wrong_schema_table,
            columns=["instrument", "timestamp", "prices", "created_at"],
        )

        # Create valid parquet data
        table = pa.table(
            {
                "instrument": pa.array(["BTCUSD"]),
                "timestamp": pa.array([datetime.now(timezone.utc)]),
                "prices": pa.array([50000.0]),
            }
        )

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # Should fail due to schema mismatch
        with pytest.raises(DatabaseError):
            loader.load(buffer)

    finally:
        client.raw_query(f"DROP TABLE IF EXISTS {wrong_schema_table}")


if __name__ == "__main__":
    # Enable logging for manual testing
    logging.basicConfig(level=logging.INFO)
    pytest.main([__file__, "-v"])
