-- models/staging/stg_stock_movements.sql
-- Cleans raw warehouse stock movement events.
-- Classifies movement direction and validates delta integrity.
{
{ config
(   
    materialized = 'incremental',   
    unique_key = 'event_id',
    tags = ['staging', 'inventory']
) }}

with
    source
    as
    (

        select *
        from {{ source
    ('raw', 'raw_stock_movements') }}

    {%
if is_incremental() %}
        where ingested_at >
(
            select max(ingested_at)
from {{ this }}
        )
{% endif %}

)

select *
from source

cleaned
as
(

    select
    event_id,
    reference_id,
    sku,
    upper(trim(category))   as category,
    upper(trim(warehouse))  as warehouse,
    lower(trim(event_type)) as movement_type,
    lower(trim(reason))     as reason,

    -- Timestamps
    ts::timestamptz         as event_ts,
    ts::date                as event_date,

    -- Stock delta with direction classification
    delta::int              as stock_delta,
    case
            when delta > 0 then 'inbound'
            when delta < 0 then 'outbound'
            else 'neutral'
        end                     as direction,
    abs(delta)              as units_moved,

    ingested_at

from source
where
        event_id is not null
    and sku  is not null
    and delta != 0

)

select *
from cleaned