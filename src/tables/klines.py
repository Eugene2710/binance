from sqlalchemy import MetaData, Table, Column, text, select
from clickhouse_sqlalchemy import types, engines, MaterializedView

metadata = MetaData()

# 1) Append-only raw feed table
klines_append_only = Table(
    "klines_append_only",
    metadata,
    Column("symbol", types.String),
    Column("open_time", types.DateTime64(3)),  # milliseconds precision
    Column("open_price", types.Float64),
    Column("high_price", types.Float64),
    Column("low_price", types.Float64),
    Column("close_price", types.Float64),
    Column("volume", types.Float64),
    Column("close_time", types.DateTime64(3)),  # ms precision
    Column("quote_asset_volume", types.Float64),
    Column("number_of_trades", types.Int64),
    Column("taker_buy_base_asset_volume", types.Float64),
    Column("taker_buy_quote_asset_volume", types.Float64),
    Column("ignore", types.String),
    Column(
        "created_at",
        types.DateTime64(3),
        server_default=text("now()"),  # clickhouse SQL DEFAULT now()
    ),
    # MergeTree engine with a monthly partition and clustered key
    engines.MergeTree(
        partition_by=text(
            "toYYYYMM(open_time)"
        ),  # must use text() for SQL expressions :contentReference[oaicite:0]{index=0}
        order_by=("symbol", "open_time"),
    ),
)

# 2) De-duplicated view table using ReplacingMergeTree
klines_rmt = Table(
    "klines_rmt",
    metadata,
    # re-declare the same columns (or import from klines_append_only.c if you prefer)
    Column("symbol", types.String),
    Column("open_time", types.DateTime64(3)),  # ms precision
    Column("open_price", types.Float64),
    Column("high_price", types.Float64),
    Column("low_price", types.Float64),
    Column("close_price", types.Float64),
    Column("volume", types.Float64),
    Column("close_time", types.DateTime64(3)),
    Column("quote_asset_volume", types.Float64),
    Column("number_of_trades", types.Int64),
    Column("taker_buy_base_asset_volume", types.Float64),
    Column("taker_buy_quote_asset_volume", types.Float64),
    Column("ignore", types.String),
    Column("created_at", types.DateTime64(3)),
    engines.ReplacingMergeTree(
        version="created_at",  # dedupe by latest created_at timestamp
        partition_by=text("toYYYYMM(open_time"),
        order_by=("symbol", "open_time"),
    ),
)

# 3) Materialized View that routes unique rows into klines_rmt
klines_mv = MaterializedView(
    klines_rmt,
    select([*klines_append_only.columns]),  # type: ignore[call-overload]
    name="klines_mv",
    use_to=True,  # generates: CREATE MATERIALIZED VIEW ... TO klines_rmt AS ...
)
