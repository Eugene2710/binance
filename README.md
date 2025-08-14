# Binance Pipeline

This project extracts and streams (in progress)

1. OrderBook information
2. KLines Candlestick Data for OCHLV data for trading pairs.

## Binance Rate Limits & Resolution

Binance has the following rate limits:
1. Hard-limits
2. ML (Machine Learning) Limits
3. WAF (Web Application Firewall) Limits

Hard Limit is the limit which will be applicable for the project thus far with the following info
- 6,000 request weight per minute (keep in mind that this is not necessarily the same as 6,000 requests)
- 100 orders per 10 seconds
- 200,000 orders per 24 hours

[Source](https://www.binance.com/en/support/faq/detail/360004492232)

[Klines weight](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data): 5 for default 500 rows at a time 

[Klines Resolution](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data): 1s

Note: To get exchange info for klines info, [ExchangeInfo API](https://developers.binance.com/docs/derivatives/option/market-data/Exchange-Information#api-description) is being used too

## Websocket Limits
- WebsSocket connections have a limit of 5 incoming messages per second where each message is considered a PING frame, PONG frame, or JSON controlled message(e.g subscribe, unsubscribe)
- Connection tha goes beyond limit will be disconnected and repeated disconnections may be banned by IP address
- A single connection can listen to a max of 1024 streams
- 300 connections per attempt every 5 minutes per IP -> approx. 1 connection per second

Source: [Binance Stream Docs](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)

### Potential Questions to consider

QQ: For 1 trading pair, how many API requests do you need to get 1 minute resolution data, for start of data 2024 and to get the data in a reasonable amount of time, let's say 3 days.

Start: 1 January 2024

End: 1 June 2025

365 + (365/2) = 547.5 days

547.5 * 24 * 60 = 788,400 minutes

Assuming sticking with a default limit of 500, number of requests = 788400 / 500 = 1577 requests.

QQ: Will it be possible to all 1577 requests within a max of 3 days?

We have 20 requests per second for Klines

Time taken to ingest in seconds = 1577 / 20 requests per second = 79 seconds

Hence, the ETL pipeline will be the limiting factor.


## Project Structure

1. ClickHouse SQL DDL Commands

2. Extractors
3. Service Level Data Classes
4. Database Level Data Classes (DTO)

## Project Setup

### 1. Install ClickHouse 25.5 and run locally

Follow the instructions [here for MacOS](https://clickhouse.com/docs/install)

Install with Brew

```commandline
brew install --cask clickhouse
```

Check if ClickHouse was installed successfully. If installed successfully, there should be a path to where ClickHouse was installed.

```commandline
which clickhouse
```

```commandline
clickhouse server
```

To connect to Local ClickHouse on Port 9000

```commandline
clickhouse client
```

### 2. Setup LocalStack for S3 and SQS Testing

This project uses LocalStack to provide local AWS services (S3 and SQS) for development and testing.

#### Start LocalStack Services

Use Docker Compose to start LocalStack with S3 and SQS services:

```bash
docker-compose -f docker-compose.yml up
```

This will start:
- **LocalStack Gateway** on `localhost:4566` (for AWS API calls)
- **MinIO Web UI** on `localhost:9001` (for visual S3 management)

#### MinIO Web UI Access

Once both services are running, you can access the MinIO web interface at:
```
http://localhost:9001
```

Login credentials:
- **Username:** minioadmin
- **Password:** minioadmin

The MinIO UI allows you to:
- Browse S3 buckets and objects visually
- Upload and download files
- Manage bucket policies and settings
- Monitor storage usage

Note: The MinIO UI connects to LocalStack's S3 service, providing a visual interface for the LocalStack S3 backend.

#### Configure AWS CLI for LocalStack

To interact with LocalStack using AWS CLI:

```bash
# Configure AWS CLI to point to LocalStack
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test
aws configure set region us-east-1

# Create a test bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket

# List buckets
aws --endpoint-url=http://localhost:4566 s3 ls
```

#### Integration Testing

The integration tests automatically configure boto3 to use LocalStack:

```python
# Tests automatically use LocalStack endpoint
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)
```

Run integration tests with:
```bash
# Make sure LocalStack is running first
docker-compose up -d

# Run the tests
PYTHONPATH=. python -m pytest integration_tests/ -v
```

<details>

<summary>Approach 2: Download the Binary and Run</summary>

Linux: Download [`clickhouse-common-static-25.5.3.75-amd64.tgz`](https://github.com/ClickHouse/ClickHouse/releases/download/v25.5.3.75-stable/clickhouse-common-static-25.5.3.75-amd64.tgz) from ClickHouse Github

Extract the zip to get both the client and server source code

```commandline
tar -xzf clickhouse-common-static-25.5.3.75-amd64
cd clickhouse-common-static-25.5.3.75-amd64
```

Spin up ClickHouse on (HTTP) Port 8123 and (Native SQL Protocol) Port 9000

```commandline
./bin/usr/clickhouse server
```     