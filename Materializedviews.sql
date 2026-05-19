-- ============================================================
-- Fashion Inventory — Warehouse Layer
-- Materialized views consumed by the BI / reporting layer.
-- Run once to set up; refreshed hourly by the Airflow DAG.
-- ============================================================


-- ─── 1. Category Daily Summary ────────────────────────────────────────────────
-- Aggregated stock, velocity, and revenue per category per day.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_category_daily_summary AS

SELECT
    inv.snapshot_date,
    inv.category,

    -- Stock metrics
    SUM(inv.stock_on_hand)                              AS total_stock,
    COUNT(DISTINCT inv.sku)                             AS active_skus,
    AVG(inv.days_on_hand)                               AS avg_days_on_hand,
    SUM(CASE WHEN inv.is_reorder_required THEN 1 END)   AS reorder_skus,
    SUM(CASE WHEN inv.is_overstock THEN 1 END)          AS overstock_skus,

    -- Velocity
    SUM(inv.units_sold)                                 AS units_sold,
    ROUND(AVG(inv.velocity_7d), 2)                      AS avg_velocity_7d,
    ROUND(AVG(inv.velocity_30d), 2)                     AS avg_velocity_30d,

    -- Revenue
    SUM(inv.gross_revenue)                              AS gross_revenue,
    SUM(inv.net_revenue)                                AS net_revenue,
    ROUND(
        (1 - SUM(inv.net_revenue) / NULLIF(SUM(inv.gross_revenue), 0)) * 100, 2
    )                                                   AS avg_discount_pct

FROM fct_inventory_snapshot inv
GROUP BY 1, 2
ORDER BY 1 DESC, gross_revenue DESC;

CREATE UNIQUE INDEX ON mv_category_daily_summary (snapshot_date, category);


-- ─── 2. SKU Reorder Watchlist ─────────────────────────────────────────────────
-- Current snapshot of all SKUs that need restocking.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sku_reorder_watchlist AS

WITH latest AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY sku ORDER BY snapshot_date DESC) AS rn
    FROM fct_inventory_snapshot
    WHERE snapshot_date = CURRENT_DATE
)

SELECT
    sku,
    category,
    stock_on_hand,
    units_sold                              AS units_sold_today,
    velocity_7d,
    velocity_30d,
    days_on_hand,
    is_reorder_required,
    is_overstock,

    -- Urgency tier
    CASE
        WHEN stock_on_hand = 0              THEN 'OUT_OF_STOCK'
        WHEN days_on_hand < 7               THEN 'CRITICAL'
        WHEN days_on_hand < 14              THEN 'WARNING'
        WHEN days_on_hand < 21              THEN 'LOW'
        ELSE 'HEALTHY'
    END                                     AS stock_status,

    -- Suggested reorder quantity (30-day supply)
    GREATEST(
        CEIL(velocity_30d * 30 - stock_on_hand),
        0
    )::INT                                  AS suggested_reorder_qty

FROM latest
WHERE rn = 1
ORDER BY days_on_hand ASC NULLS FIRST;

CREATE UNIQUE INDEX ON mv_sku_reorder_watchlist (sku);


-- ─── 3. Channel Revenue Weekly ────────────────────────────────────────────────
-- Weekly net revenue and unit breakdown by sales channel.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_channel_revenue_weekly AS

SELECT
    DATE_TRUNC('week', event_date)::DATE    AS week_start,
    channel,
    category,
    COUNT(DISTINCT sku)                     AS active_skus,
    SUM(quantity)                           AS units_sold,
    SUM(net_revenue)                        AS net_revenue,
    SUM(gross_revenue)                      AS gross_revenue,
    ROUND(AVG(discount_pct), 2)             AS avg_discount_pct,
    COUNT(DISTINCT transaction_id)          AS transactions,
    ROUND(SUM(net_revenue) /
          NULLIF(COUNT(DISTINCT transaction_id), 0), 2)  AS avg_basket_value

FROM stg_sales
GROUP BY 1, 2, 3
ORDER BY 1 DESC, net_revenue DESC;

CREATE UNIQUE INDEX ON mv_channel_revenue_weekly (week_start, channel, category);


-- ─── 4. Supplier On-Time Delivery ─────────────────────────────────────────────
-- Measures supplier reliability and lead time.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_supplier_performance AS

SELECT
    supplier_id,
    DATE_TRUNC('month', ts)::DATE           AS month,
    COUNT(*)                                AS total_deliveries,
    SUM(quantity)                           AS total_units_received,
    SUM(quantity * unit_cost)               AS total_cost,

    -- On-time delivery rate
    ROUND(
        SUM(CASE WHEN actual_date <= expected_date THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*) * 100, 1
    )                                       AS on_time_pct,

    -- Average lead time variance (days late; negative = early)
    ROUND(AVG(actual_date - expected_date), 1) AS avg_lead_variance_days

FROM raw_supplier_deliveries
WHERE actual_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 2 DESC, on_time_pct ASC;

CREATE UNIQUE INDEX ON mv_supplier_performance (supplier_id, month);


-- ─── 5. KPI Dashboard View ────────────────────────────────────────────────────
-- Single-row daily KPI summary for the executive dashboard.

CREATE OR REPLACE VIEW vw_daily_kpis AS

SELECT
    CURRENT_DATE                                                    AS kpi_date,

    -- Inventory health
    (SELECT SUM(stock_on_hand)   FROM mv_sku_reorder_watchlist)     AS total_units_on_hand,
    (SELECT COUNT(DISTINCT sku)  FROM mv_sku_reorder_watchlist)     AS total_active_skus,
    (SELECT COUNT(*)             FROM mv_sku_reorder_watchlist
     WHERE stock_status IN ('CRITICAL','OUT_OF_STOCK'))             AS critical_sku_count,
    (SELECT ROUND(AVG(days_on_hand),1) FROM mv_sku_reorder_watchlist
     WHERE days_on_hand IS NOT NULL)                                AS avg_days_on_hand,

    -- Today's revenue
    (SELECT SUM(net_revenue)  FROM stg_sales
     WHERE event_date = CURRENT_DATE)                               AS revenue_today,
    (SELECT SUM(quantity)     FROM stg_sales
     WHERE event_date = CURRENT_DATE)                               AS units_sold_today,

    -- Fill rate: SKUs with stock > 0 / total SKUs
    ROUND(
        (SELECT COUNT(*) FROM mv_sku_reorder_watchlist WHERE stock_on_hand > 0)::NUMERIC
        / NULLIF((SELECT COUNT(*) FROM mv_sku_reorder_watchlist), 0) * 100, 1
    )                                                               AS fill_rate_pct;