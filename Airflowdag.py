"""
Fashion Store Inventory — Airflow DAG
Orchestrates: raw data quality checks → dbt staging → dbt marts → alert dispatch.
Schedule: every hour during business hours, full refresh nightly at 02:00 IST.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable


# ─── Default Args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["de-alerts@fashionstore.in"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

DBT_DIR      = "/opt/airflow/dbt/fashion_inventory"
DBT_PROFILES = "/opt/airflow/dbt/profiles.yml"
DBT_CMD      = f"dbt --no-use-colors --profiles-dir {DBT_PROFILES} --project-dir {DBT_DIR}"


# ─── Python Callables ──────────────────────────────────────────────────────────

def check_raw_data_quality(**ctx):
    """
    Validate row counts and freshness of raw staging tables.
    Raises if data is stale (>2h) or counts are suspiciously low.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=Variable.get("db_host"),
        dbname=Variable.get("db_name"),
        user=Variable.get("db_user"),
        password=Variable.get("db_password"),
    )

    checks = {
        "raw_sales":              "SELECT COUNT(*) FROM raw_sales WHERE ingested_at > NOW() - INTERVAL '2 hours'",
        "raw_stock_movements":    "SELECT COUNT(*) FROM raw_stock_movements WHERE ingested_at > NOW() - INTERVAL '2 hours'",
        "raw_supplier_deliveries":"SELECT COUNT(*) FROM raw_supplier_deliveries WHERE ingested_at > NOW() - INTERVAL '2 hours'",
    }

    failures = []
    with conn.cursor() as cur:
        for table, query in checks.items():
            cur.execute(query)
            count = cur.fetchone()[0]
            if count < 1:
                failures.append(f"{table}: only {count} fresh rows (expected ≥1)")

    conn.close()
    if failures:
        raise ValueError(f"Data quality failures:\n" + "\n".join(failures))

    print(f"All raw tables passed freshness checks.")


def decide_run_mode(**ctx):
    """Branch: full-refresh at 02:00, incremental otherwise."""
    hour = ctx["logical_date"].hour
    return "dbt_full_refresh" if hour == 2 else "dbt_incremental"


def dispatch_reorder_alerts(**ctx):
    """
    Query reorder_alerts table and push unresolved alerts to Slack / email.
    In production, replace with actual webhook/SMTP calls.
    """
    import psycopg2, json

    conn = psycopg2.connect(
        host=Variable.get("db_host"),
        dbname=Variable.get("db_name"),
        user=Variable.get("db_user"),
        password=Variable.get("db_password"),
    )

    with conn.cursor() as cur:
        cur.execute("""
            SELECT sku, category, threshold, current_stock, alert_ts
            FROM reorder_alerts
            WHERE resolved = FALSE
            ORDER BY current_stock ASC
            LIMIT 50
        """)
        rows = cur.fetchall()

    conn.close()

    if not rows:
        print("No unresolved reorder alerts.")
        return

    alert_payload = [
        {"sku": r[0], "category": r[1], "threshold": r[2],
         "current_stock": r[3], "alert_ts": str(r[4])}
        for r in rows
    ]

    # In production: requests.post(SLACK_WEBHOOK, json={"text": json.dumps(alert_payload)})
    print(f"Would dispatch {len(alert_payload)} alerts:\n{json.dumps(alert_payload[:3], indent=2)}")


def update_bi_cache(**ctx):
    """Refresh materialized views consumed by the BI layer."""
    import psycopg2

    conn = psycopg2.connect(
        host=Variable.get("db_host"),
        dbname=Variable.get("db_name"),
        user=Variable.get("db_user"),
        password=Variable.get("db_password"),
    )

    views = [
        "mv_category_daily_summary",
        "mv_sku_reorder_watchlist",
        "mv_channel_revenue_weekly",
    ]

    with conn.cursor() as cur:
        for view in views:
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
            print(f"Refreshed: {view}")
    conn.commit()
    conn.close()


# ─── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "fashion_inventory_pipeline",
    default_args    = DEFAULT_ARGS,
    description     = "End-to-end fashion inventory data pipeline",
    schedule_interval = "0 * * * *",           # hourly
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["inventory", "fashion", "data-engineering"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Raw data quality gate
    dq_check = PythonOperator(
        task_id         = "raw_data_quality_check",
        python_callable = check_raw_data_quality,
    )

    # 2. Branch: full-refresh vs incremental
    branch = BranchPythonOperator(
        task_id         = "decide_run_mode",
        python_callable = decide_run_mode,
    )

    # 3a. Full refresh (nightly)
    dbt_full_refresh = BashOperator(
        task_id  = "dbt_full_refresh",
        bash_command = f"{DBT_CMD} run --full-refresh --select staging --target prod",
    )

    # 3b. Incremental run (hourly)
    dbt_incremental = BashOperator(
        task_id  = "dbt_incremental",
        bash_command = f"{DBT_CMD} run --select staging --target prod",
    )

    # 4. dbt tests on staging
    dbt_test_staging = BashOperator(
        task_id      = "dbt_test_staging",
        bash_command = f"{DBT_CMD} test --select staging --target prod",
        trigger_rule = TriggerRule.ONE_SUCCESS,
    )

    # 5. Build mart models
    dbt_marts = BashOperator(
        task_id      = "dbt_build_marts",
        bash_command = f"{DBT_CMD} run --select marts --target prod",
    )

    # 6. dbt tests on marts
    dbt_test_marts = BashOperator(
        task_id      = "dbt_test_marts",
        bash_command = f"{DBT_CMD} test --select marts --target prod",
    )

    # 7. Dispatch reorder alerts
    alerts = PythonOperator(
        task_id         = "dispatch_reorder_alerts",
        python_callable = dispatch_reorder_alerts,
    )

    # 8. Refresh BI materialized views
    bi_refresh = PythonOperator(
        task_id         = "refresh_bi_cache",
        python_callable = update_bi_cache,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    # ─── Wiring ───────────────────────────────────────────────────────────────
    (
        start
        >> dq_check
        >> branch
        >> [dbt_full_refresh, dbt_incremental]
        >> dbt_test_staging
        >> dbt_marts
        >> dbt_test_marts
        >> [alerts, bi_refresh]
        >> end
    )