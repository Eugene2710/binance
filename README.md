# Binance Pipeline

This project extracts

1. OrderBook information
2. KLines Candlestick Data for OCHLV data for trading pairs.

## Project Structure

1. ClickHouse SQL DDL Commands

2. Extractors
3. Service Level Data Classes
4. Database Level Data Classes (DTO)

## Project Setup
1. Install ClickHouse 25.5 and run locally

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