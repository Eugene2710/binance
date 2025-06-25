-- Change Engine to ReplicatedMergeTree when hosting on cloud
-- ETL pipeline inserts into append_only table
-- Do not query append_only table directly. It can contain duplicates
-- The materialized view listens for inserts to append_only, and moves only unique rows to klines_rmt
CREATE TABLE klines_append_only(
    symbol String,
    open_time DateTime64(3),    -- ms precision, unix timestamp
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    close_time DateTime64(3),   -- ms precision, unix timestamp
    quote_asset_volume Float64,
    number_of_trades Int64,
    taker_buy_base_asset_volume Float64,
    taker_buy_quote_asset_volume Float64,
    ignore String,
    created_at DateTime64(3) DEFAULT now()  -- Time data was inserted into DB
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time);

-- Server queries this rmt table. This contains distinct rows by symbol, open_time primary key.
CREATE TABLE klines_rmt AS klines_append_only
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time);

-- Inserts the same row to klines_mv whenever a row is inserted into klines_append_only.
CREATE MATERIALIZED VIEW klines_mv TO klines_rmt
AS
SELECT * from klines_append_only;