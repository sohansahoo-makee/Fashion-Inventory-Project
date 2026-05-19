"""
Fashion Store Inventory - Stream Processor
Consumes Kafka topics, applies business logic (deduplication,
enrichment, threshold checks), and sinks to PostgreSQL staging tables.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ─── Config ───────────────────────────────────────────────────────────────────

KAFKA_BROKER  = "localhost:9092"
GROUP_ID      = "fashion-inventory-processor"
TOPICS        = ["fashion.pos.sales", "fashion.warehouse.stock_movement", "fashion.supplier.deliveries"]

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "fashion_inventory",
    "user": "pipeline_user", "password": "pipeline_pass",
}

REORDER_THRESHOLDS: Dict[str, int] = {
    "Dresses": 30, "Tops": 50, "Denim": 25,
    "Footwear": 20, "Accessories": 60, "Outerwear": 15, "Swimwear": 10,
}


# ─── DB Helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def ensure_tables(conn):
    """Create raw staging tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_sales (
                event_id      TEXT PRIMARY KEY,
                event_type    TEXT,
                ts            TIMESTAMPTZ,
                sku           TEXT,
                category      TEXT,
                channel       TEXT,
                quantity      INT,
                unit_price    NUMERIC(12,2),
                discount_pct  NUMERIC(5,2),
                size          TEXT,
                color         TEXT,
                transaction_id TEXT,
                ingested_at   TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS raw_stock_movements (
                event_id     TEXT PRIMARY KEY,
                event_type   TEXT,
                ts           TIMESTAMPTZ,
                sku          TEXT,
                category     TEXT,
                warehouse    TEXT,
                delta        INT,
                reason       TEXT,
                reference_id TEXT,
                ingested_at  TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS raw_supplier_deliveries (
                event_id      TEXT PRIMARY KEY,
                event_type    TEXT,
                ts            TIMESTAMPTZ,
                sku           TEXT,
                category      TEXT,
                supplier_id   TEXT,
                po_number     TEXT,
                quantity      INT,
                unit_cost     NUMERIC(12,2),
                expected_date DATE,
                actual_date   DATE,
                ingested_at   TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS reorder_alerts (
                sku           TEXT,
                category      TEXT,
                threshold     INT,
                current_stock INT,
                alert_ts      TIMESTAMPTZ DEFAULT NOW(),
                resolved      BOOLEAN DEFAULT FALSE
            );
        """)
    conn.commit()


# ─── Event Handlers ────────────────────────────────────────────────────────────

def handle_sale(msg: Dict[str, Any], cur) -> None:
    cur.execute("""
        INSERT INTO raw_sales
            (event_id, event_type, ts, sku, category, channel,
             quantity, unit_price, discount_pct, size, color, transaction_id)
        VALUES (%(event_id)s, %(event_type)s, %(timestamp)s, %(sku)s,
                %(category)s, %(channel)s, %(quantity)s, %(unit_price)s,
                %(discount_pct)s, %(size)s, %(color)s, %(transaction_id)s)
        ON CONFLICT (event_id) DO NOTHING;
    """, msg)

def handle_stock_movement(msg: Dict[str, Any], cur) -> None:
    cur.execute("""
        INSERT INTO raw_stock_movements
            (event_id, event_type, ts, sku, category, warehouse, delta, reason, reference_id)
        VALUES (%(event_id)s, %(event_type)s, %(timestamp)s, %(sku)s,
                %(category)s, %(warehouse)s, %(delta)s, %(reason)s, %(reference_id)s)
        ON CONFLICT (event_id) DO NOTHING;
    """, msg)
    _check_reorder_threshold(msg, cur)

def handle_supplier_delivery(msg: Dict[str, Any], cur) -> None:
    cur.execute("""
        INSERT INTO raw_supplier_deliveries
            (event_id, event_type, ts, sku, category, supplier_id, po_number,
             quantity, unit_cost, expected_date, actual_date)
        VALUES (%(event_id)s, %(event_type)s, %(timestamp)s, %(sku)s,
                %(category)s, %(supplier_id)s, %(po_number)s, %(quantity)s,
                %(unit_cost)s, %(expected_date)s, %(actual_date)s)
        ON CONFLICT (event_id) DO NOTHING;
    """, msg)

def _check_reorder_threshold(msg: Dict[str, Any], cur) -> None:
    """Real-time reorder alert — fires when stock delta pushes below threshold."""
    threshold = REORDER_THRESHOLDS.get(msg.get("category", ""), 20)
    cur.execute("""
        SELECT COALESCE(SUM(delta), 0) FROM raw_stock_movements WHERE sku = %s
    """, (msg["sku"],))
    current_stock = cur.fetchone()[0]
    if current_stock < threshold:
        cur.execute("""
            INSERT INTO reorder_alerts (sku, category, threshold, current_stock)
            VALUES (%s, %s, %s, %s)
        """, (msg["sku"], msg["category"], threshold, current_stock))
        log.warning(f"REORDER ALERT | {msg['sku']} stock={current_stock} threshold={threshold}")


# ─── Router ───────────────────────────────────────────────────────────────────

HANDLERS = {
    "fashion.pos.sales":                handle_sale,
    "fashion.warehouse.stock_movement": handle_stock_movement,
    "fashion.supplier.deliveries":      handle_supplier_delivery,
}


# ─── Consumer Loop ─────────────────────────────────────────────────────────────

def run():
    conn = get_conn()
    ensure_tables(conn)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=500,
        session_timeout_ms=30000,
    )

    log.info(f"Subscribed to: {TOPICS}")
    batch = []

    try:
        for message in consumer:
            topic   = message.topic
            payload = message.value

            with conn.cursor() as cur:
                handler = HANDLERS.get(topic)
                if handler:
                    handler(payload, cur)
                else:
                    log.warning(f"No handler for topic: {topic}")
            conn.commit()
            consumer.commit()

            log.info(f"[{topic}] sku={payload.get('sku')} event={payload.get('event_type')}")

    except KeyboardInterrupt:
        log.info("Shutting down consumer.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run()