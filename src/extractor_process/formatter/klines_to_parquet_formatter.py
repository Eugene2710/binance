import io
from src.extractor_process.formatter.base_formatter import Formatter
from src.common.models.service_level.binance_klines import Klines
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq


class KLinesToParquetFormatter(Formatter[Klines]):
    PYARROW_SCHEMA = pa.schema(
        [
            pa.field("symbol", pa.string()),
            pa.field("open_time", pa.timestamp("ms")),
            pa.field("open_price", pa.string()),
            pa.field("high_price", pa.string()),
            pa.field("low_price", pa.string()),
            pa.field("close_price", pa.string()),
            pa.field("volume", pa.string()),
            pa.field("close_time", pa.int64()),
            pa.field("quote_asset_volume", pa.string()),
            pa.field("number_of_trades", pa.int64()),
            pa.field("taker_buy_base_asset_volume", pa.string()),
            pa.field("taker_buy_quote_asset_volume", pa.string()),
            pa.field("ignore", pa.string()),
        ]
    )

    @classmethod
    def format(cls, data: Klines) -> io.BytesIO | None:
        """
        Extract single KLine from KLines -> Each KLine will be a row
        - A row is analogous to a dict[str, Any]
        Write the data into an in-memory pyarrow parquet
        Write the pyarrow parquet into a io.BytesIO (In memory binary encoded buffer)

        If there are no kline (meaning no rows), return None
        We don't want to upload this to S3 anyway
        """
        rows: list[dict[str, Any]] = [m.model_dump() for m in data.klines]
        if not rows:
            return None
        table: pa.Table = pa.Table.from_pylist(rows, schema=cls.PYARROW_SCHEMA)
        buffer: io.BytesIO = io.BytesIO()
        pq.write_table(table, buffer, compression="zstd")
        buffer.seek(0)
        return buffer
