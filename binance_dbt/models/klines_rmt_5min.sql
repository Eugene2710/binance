{{ config(
    materialized = "incremental",
    unique_key = ["symbol", "open_time"],
    alias = "klines_rmt_5min"
) }}

with raw as (
    select *
    from {{ source('resources', 'klines_rmt') }}
    FINAL
),
bucketed as (
    select
        symbol,
        toStartOfInterval(open_time, INTERVAL 5 MINUTE) as bucket_start,
        open_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        close_time,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume,
        ignore,
        created_at
    from raw
    {% if is_incremental() %}
      where open_time > (select max(open_time) from {{ this }})
    {% endif %}
),
agg as (
    select
        symbol,
        bucket_start as open_time,
        argMax(open_price, open_time) as open_price,
        argMax(high_price, open_time) as high_price,
        argMax(low_price, open_time) as low_price,
        argMax(close_price, open_time) as close_price,
        argMax(volume, open_time) as volume,
        argMax(close_time, open_time) as close_time,
        argMax(quote_asset_volume, open_time) as quote_asset_volume,
        argMax(number_of_trades, open_time) as number_of_trades,
        argMax(taker_buy_base_asset_volume, open_time) as taker_buy_base_asset_volume,
        argMax(taker_buy_quote_asset_volume, open_time) as taker_buy_quote_asset_volume,
        argMax(ignore, open_time) as ignore,
        argMax(created_at, open_time) as created_at
    from bucketed
    group by
        symbol, bucket_start
)

select * from agg