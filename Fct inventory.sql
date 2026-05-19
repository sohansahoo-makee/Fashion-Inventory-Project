-- models/marts/fct_inventory_snapshot.sql
-- Daily inventory snapshot per SKU.
-- Combines sales outflows and stock movement deltas to compute
-- running stock levels, days-on-hand, and reorder flags.

{{config(
    materialized = 'table',     
    tags = ['mart', 'inventory'],
    indexes = [
        {'columns': ['snapshot_date', 'sku'], 'unique': true},
        {'columns': ['category']},
        {'columns': ['is_reorder_required']}
    ]
) }}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart = 'day',
        start_date = "cast('2024-01-01' as date)",
        end_date = "current_date + interval '1 day'"
    ) }}

),

skus as
(

    select distinct sku, category
from {{ ref
('your_sku_model') }}

)

select *
from date_spine
cross join skus

-- All movement deltas per SKU per day (positive = stock in, negative = stock out)
daily_movements
as
(

    select
    event_date                  as movement_date,
    sku,
    category,
    sum(stock_delta)            as net_delta
from {{ ref
('stg_stock_movements') }}
    group by 1, 2, 3

),

-- Sales outflows per SKU per day
daily_sales_outflow as
(

    select
    event_date                  as sale_date,
    sku,
    category,
    sum(quantity)               as units_sold,
    sum(gross_revenue)          as gross_revenue,
    sum(net_revenue)            as net_revenue
from {{ ref
('stg_sales') }}
    group by 1, 2, 3

),

-- Spine: every (sku, date) combination
sku_date_spine as
(

    select
    d.date_day::date            as snapshot_date,
    s.sku,
    s.category
from date_spine d
    cross join skus s

)
,

joined as
(

    select
    sp.snapshot_date,
    sp.sku,
    sp.category,
    coalesce(m.net_delta, 0)    as stock_received,
    coalesce(so.units_sold, 0)  as units_sold,
    coalesce(so.gross_revenue, 0) as gross_revenue,
    coalesce(so.net_revenue, 0)  as net_revenue

from sku_date_spine sp
    left join daily_movements       m on sp.sku = m.sku and sp.snapshot_date = m.movement_date
    left join daily_sales_outflow   so on sp.sku = so.sku and sp.snapshot_date = so.sale_date

)
,

with_running_stock as
(

    select
    *,
    -- Running cumulative stock (movements minus sales)
    sum(stock_received - units_sold)
            over (partition by sku order by snapshot_date
                  rows between unbounded preceding and current row)   as stock_on_hand,

    -- 7-day rolling sales velocity
    avg(units_sold)
            over (partition by sku order by snapshot_date
                  rows between 6 preceding and current row)           as velocity_7d,

    -- 30-day rolling sales velocity
    avg(units_sold)
            over (partition by sku order by snapshot_date
                  rows between 29 preceding and current row)          as velocity_30d

from joined

)
,

final as
(

    select
    snapshot_date,
    sku,
    category,
    greatest(stock_on_hand, 0)  as stock_on_hand,
    units_sold,
    gross_revenue,
    net_revenue,

    round(velocity_7d,  2)      as velocity_7d,
    round(velocity_30d, 2)      as velocity_30d,

    -- Days on hand using 30d velocity (avoid divide-by-zero)
    case
            when velocity_30d > 0
            then round(greatest(stock_on_hand, 0) / velocity_30d, 1)
            else null
        end                         as days_on_hand,

    -- Reorder flag: stock below 21-day coverage
    case
            when velocity_30d > 0
        and greatest(stock_on_hand, 0) / velocity_30d < 21
            then true
            else false
        end                         as is_reorder_required,

    -- Overstock flag: more than 90 days of coverage
    case
            when velocity_30d > 0
        and greatest(stock_on_hand, 0) / velocity_30d > 90
            then true
            else false
        end                         as is_overstock

from with_running_stock
where snapshot_date <= current_date

)

select *
from final