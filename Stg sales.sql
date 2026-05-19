-- models/staging/stg_sales.sql
-- Cleans and casts raw POS sales events from the Kafka sink table.
-- Applies deduplication, null guards, and revenue calculation.

{{ config(
    materialized = 'incremental',
    unique_key   = 'event_id',
    on_schema_change = 'sync_all_columns',
    tags         = ['staging', 'sales']
) }}

with source as (

    select * from {{ source('raw', 'raw_sales') }}

    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}

),

cleaned as (

    select
        event_id,
        transaction_id,
        sku,
        upper(trim(category))                                   as category,
        lower(trim(channel))                                    as channel,
        size,
        color,

        -- Timestamps
        ts::timestamptz                                         as event_ts,
        date_trunc('day', ts)::date                            as event_date,
        extract(week from ts)::int                              as event_week,
        extract(month from ts)::int                            as event_month,
        extract(year from ts)::int                             as event_year,

        -- Financials
        quantity::int                                           as quantity,
        unit_price::numeric(12,2)                              as unit_price,
        coalesce(discount_pct, 0)::numeric(5,2)               as discount_pct,

        -- Derived: gross and net revenue
        round(quantity * unit_price, 2)                        as gross_revenue,
        round(quantity * unit_price * (1 - discount_pct/100), 2) as net_revenue,

        ingested_at

    from source
    where
        event_id      is not null
        and sku       is not null
        and quantity  > 0
        and unit_price > 0

)

select * from cleaned