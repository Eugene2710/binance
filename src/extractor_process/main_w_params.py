import io
from src.extractor_process.extractors.batching.get_klines import BinanceKlinesExtractor
from src.extractor_process.formatter.klines_to_parquet_formatter import (
    KLinesToParquetFormatter,
)
from src.extractor_process.s3_writer.s3_writer import S3Writer
from src.extractor_process.config import DataSourceConfig
from src.common.models.service_level.binance_klines import Klines
from src.common.logging import setup_logger
from datetime import datetime
import click
import asyncio

logger = setup_logger(__name__)


class KLinesExtractProcess:
    """
    Main process class that orchestrates KLines data extraction, formatting, and uploading to S3
    This process include passing in of a param, using click, which is the trading pair, e.g USDCBTC
    """

    def __init__(
        self,
        extractor: BinanceKlinesExtractor,
        formatter: KLinesToParquetFormatter,
        s3_writer: S3Writer,
        config: DataSourceConfig,
    ):
        self.extractor = extractor
        self.formatter = formatter
        self.s3_writer = s3_writer
        self.config = config

    async def run(
        self,
        symbol: str,
        interval: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
        current_time: datetime | None = None,
    ) -> None:
        """
        Run the complete KLines extraction process:
        1. Extract KLines data using the extractor
        2. Format the data to Parquet using the formatter
        3. Upload to S3 using the S3Writer
        """
        try:
            logger.info(
                f"Starting KLines extraction for {symbol} with interval {interval}"
            )

            # Step 1: Extract KLines data
            klines: Klines = await self.extractor.extract(
                symbol=symbol,
                interval=interval,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )

            logger.info(f"Extracted {len(klines.klines)} KLines records")

            # Step 2: Format to Parquet BytesIO buffer
            buffer: io.BytesIO | None = self.formatter.format(klines)

            if buffer is None:
                logger.warning("No data to upload - formatter returned None")
                return

            logger.info("Successfully formatted KLines data to Parquet")

            # Step 3: Generate S3 path with timestamp using config
            if current_time is None:
                current_time = datetime.utcnow()
            timestamp = current_time.strftime("%Y%m%d%H%M%S%f")
            filename = f"btcusd_{timestamp}.parquet"
            s3_path = f"{self.config.source_path}/{filename}"

            # Step 4: Upload to S3
            self.s3_writer.upload_fileobj(
                bucket_name=self.config.bucket_name, path=s3_path, buffer=buffer
            )

            logger.info(
                f"Successfully uploaded KLines data to s3://{self.config.bucket_name}/{s3_path}"
            )

        except Exception as e:
            logger.exception(f"Failed to process KLines extraction: {e}")
            raise


# async def main():
#     """
#     Main method for local testing of KLinesExtractProcess
#     """
#     # Initialize dependencies
#     extractor = BinanceKlinesExtractor()
#     formatter = KLinesToParquetFormatter()
#     s3_writer = S3Writer()
#
#     # Configure data source (using environment variables for LocalStack)
#     config = DataSourceConfig(
#         bucket_name="crypto", source_path="data_sources/klines_pricing/btcusd"
#     )
#
#     # Create the extract process
#     extract_process = KLinesExtractProcess(
#         extractor=extractor, formatter=formatter, s3_writer=s3_writer, config=config
#     )
#
#     # Run extraction for BTCUSDT with recent data
#     logger.info("Starting local test run of KLinesExtractProcess")
#
#     await extract_process.run(
#         symbol="BTCUSDT",
#         interval="1m",
#         start_time=datetime(2024, 1, 1, 0, 0),
#         end_time=datetime(2025, 1, 1, 0, 0),
#         limit=100000,
#     )
#
#     logger.info("Local test run completed successfully")

@click.command()
@click.option("--symbol", type=str, required=True)
@click.option("--interval", type=str, required=True)
@click.option("--start_time", type=click.DateTime(), required=True)
@click.option("--end_time", type=click.DateTime(), required=True)
@click.option("--limit", type=int, required=True)
def run(symbol: str, interval: str, start_time: datetime, end_time: datetime, limit: int):
    # Initialize dependencies
    extractor: BinanceKlinesExtractor = BinanceKlinesExtractor()
    formatter: KLinesToParquetFormatter = KLinesToParquetFormatter()
    s3_writer: S3Writer = S3Writer()

    # Configure data source (using environment variables for LocalStack)
    config = DataSourceConfig(
        bucket_name="crypto", source_path=f"data_sources/klines_pricing/{symbol}"
    )
    # Create the extract process
    extract_process = KLinesExtractProcess(
        extractor=extractor, formatter=formatter, s3_writer=s3_writer, config=config
    )
    logger.info("Starting local test run of KLinesExtractProcess")

    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    event_loop.run_until_complete(extract_process.run(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    ))


if __name__ == "__main__":
    run()