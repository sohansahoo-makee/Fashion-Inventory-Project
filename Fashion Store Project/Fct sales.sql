-- models/marts/fct_sales_velocity.sql
-- Aggregated sales velocity, revenue, and sell-through rate per category per week.
-- Powers the BI dashboard trend charts and category performance views.

{{ config(
    materialized = 'table',
    tags         = ['mart', 'sales']
) }}

with weekly_sales as (

    select
        date_trunc('week', event_date)::date    as week_start,
        category,
        channel,
        sku,
        sum(quantity)                           as units_sold,
        sum(net_revenue)                        as net_revenue,
        count(distinct transaction_id)          as transactions,
        avg(discount_pct)                       as avg_discount_pct

    from {{ ref('stg_sales') }}
    group by 1, 2, 3, 4

),

weekly_inventory as (

    select
        date_trunc('week', snapshot_date)::date  as week_start,
        category,
        sku,
        -- Take last snapshot of the week for closing stock
        last_value(stock_on_hand) over (
            partition by sku, date_trunc('week', snapshot_date)
            order by snapshot_date
            rows between unbounded preceding and unbounded following
        )                                        as closing_stock,

        -- Opening stock (first snapshot of week)
        first_value(stock_on_hand) over (
            partition by sku, date_trunc('week', snapshot_date)
            order by snapshot_date
            rows between unbounded preceding and unbounded following
        )                                        as opening_stock

    from {{ ref('fct_inventory_snapshot') }}

),

deduped_inventory as (
    select distinct week_start, sku, category, opening_stock, closing_stock
    from weekly_inventory
),

joined as (

    select
        ws.week_start,
        ws.category,
        ws.channel,
        ws.sku,
        ws.units_sold,
        ws.net_revenue,
        ws.transactions,
        round(ws.avg_discount_pct, 2)                               as avg_discount_pct,
        coalesce(di.opening_stock, 0)                               as opening_stock,
        coalesce(di.closing_stock, 0)                               as closing_stock,

        -- Sell-through rate = units sold / opening stock
        case
            when coalesce(di.opening_stock, 0) > 0
            then round(ws.units_sold::numeric / di.opening_stock * 100, 1)
            else null
        end                                                         as sell_through_pct,

        -- Revenue per unit
        case
            when ws.units_sold > 0
            then round(ws.net_revenue / ws.units_sold, 2)
            else null
        end                                                         as revenue_per_unit,

        -- Week-over-week units sold change
        ws.units_sold - lag(ws.units_sold) over (
            partition by ws.sku, ws.channel
            order by ws.week_start
        )                                                           as wow_units_delta,

        -- Week-over-week revenue change
        ws.net_revenue - lag(ws.net_revenue) over (
            partition by ws.sku, ws.channel
            order by ws.week_start
        )                                                           as wow_revenue_delta

    from weekly_sales ws
    left join deduped_inventory di
        on ws.sku = di.sku and ws.week_start = di.week_start

)

select * from joined
order by week_start desc, net_revenue desc