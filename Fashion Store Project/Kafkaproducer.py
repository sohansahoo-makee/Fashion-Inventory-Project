"""
Fashion Store Inventory - Kafka Producer
Simulates real-time POS sales events and stock movement events
from multiple store locations and e-commerce channels.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from kafka import KafkaProducer


# ─── Config ───────────────────────────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPICS = {
    "sales":          "fashion.pos.sales",
    "stock_movement": "fashion.warehouse.stock_movement",
    "returns":        "fashion.pos.returns",
    "supplier":       "fashion.supplier.deliveries",
}

CATEGORIES = ["Dresses", "Tops", "Denim", "Footwear", "Accessories", "Outerwear", "Swimwear"]
CHANNELS   = ["store_mumbai", "store_delhi", "store_bangalore", "ecommerce", "store_pune"]
SIZES      = ["XS", "S", "M", "L", "XL", "XXL"]
COLORS     = ["Black", "White", "Navy", "Beige", "Olive", "Rust", "Sage"]

SKUS = [
    {"sku": f"{cat[:2].upper()}-{str(i).zfill(4)}", "category": cat, "base_price": random.uniform(800, 8000)}
    for cat in CATEGORIES
    for i in range(1, 21)
]


# ─── Event Schemas ─────────────────────────────────────────────────────────────

@dataclass
class SaleEvent:
    event_id:     str
    event_type:   str          # "sale"
    timestamp:    str
    sku:          str
    category:     str
    channel:      str
    quantity:     int
    unit_price:   float
    discount_pct: float
    size:         str
    color:        str
    transaction_id: str

@dataclass
class StockMovementEvent:
    event_id:      str
    event_type:    str          # "adjustment" | "transfer" | "writeoff"
    timestamp:     str
    sku:           str
    category:      str
    warehouse:     str
    delta:         int          # positive = stock in, negative = stock out
    reason:        str
    reference_id:  str

@dataclass
class SupplierDeliveryEvent:
    event_id:       str
    event_type:     str         # "delivery"
    timestamp:      str
    sku:            str
    category:       str
    supplier_id:    str
    po_number:      str
    quantity:       int
    unit_cost:      float
    expected_date:  str
    actual_date:    Optional[str]


# ─── Event Generators ──────────────────────────────────────────────────────────

def gen_sale() -> SaleEvent:
    sku_info = random.choice(SKUS)
    return SaleEvent(
        event_id=str(uuid.uuid4()),
        event_type="sale",
        timestamp=datetime.now(timezone.utc).isoformat(),
        sku=sku_info["sku"],
        category=sku_info["category"],
        channel=random.choice(CHANNELS),
        quantity=random.randint(1, 3),
        unit_price=round(sku_info["base_price"] * random.uniform(0.9, 1.1), 2),
        discount_pct=random.choice([0, 0, 0, 5, 10, 15, 20, 30]),
        size=random.choice(SIZES),
        color=random.choice(COLORS),
        transaction_id=str(uuid.uuid4()),
    )

def gen_stock_movement() -> StockMovementEvent:
    sku_info = random.choice(SKUS)
    event_type = random.choices(
        ["adjustment", "transfer", "writeoff"],
        weights=[0.6, 0.3, 0.1]
    )[0]
    delta = random.randint(-50, 200) if event_type == "adjustment" else random.randint(-20, 20)
    return StockMovementEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        timestamp=datetime.now(timezone.utc).isoformat(),
        sku=sku_info["sku"],
        category=sku_info["category"],
        warehouse=random.choice(["WH-NORTH", "WH-SOUTH", "WH-WEST"]),
        delta=delta,
        reason=random.choice(["cycle_count", "damage", "theft", "supplier_return", "inter_store"]),
        reference_id=str(uuid.uuid4()),
    )

def gen_supplier_delivery() -> SupplierDeliveryEvent:
    sku_info = random.choice(SKUS)
    now = datetime.now(timezone.utc)
    return SupplierDeliveryEvent(
        event_id=str(uuid.uuid4()),
        event_type="delivery",
        timestamp=now.isoformat(),
        sku=sku_info["sku"],
        category=sku_info["category"],
        supplier_id=f"SUP-{random.randint(100, 120)}",
        po_number=f"PO-{random.randint(10000, 99999)}",
        quantity=random.randint(50, 500),
        unit_cost=round(sku_info["base_price"] * 0.4 * random.uniform(0.9, 1.1), 2),
        expected_date=now.date().isoformat(),
        actual_date=now.date().isoformat() if random.random() > 0.2 else None,
    )


# ─── Producer ──────────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        compression_type="gzip",
    )

def publish(producer: KafkaProducer, topic: str, key: str, payload: dict):
    future = producer.send(topic, key=key, value=payload)
    record_metadata = future.get(timeout=10)
    print(f"[{record_metadata.topic}] partition={record_metadata.partition} offset={record_metadata.offset} | {key}")

def run(events_per_second: int = 5, duration_seconds: int = 60):
    producer = create_producer()
    print(f"Publishing ~{events_per_second} events/sec for {duration_seconds}s ...")

    end_time = time.time() + duration_seconds
    while time.time() < end_time:
        # Sales are most frequent
        for _ in range(events_per_second):
            roll = random.random()
            if roll < 0.65:
                event = gen_sale()
                publish(producer, TOPICS["sales"], event.sku, asdict(event))
            elif roll < 0.85:
                event = gen_stock_movement()
                publish(producer, TOPICS["stock_movement"], event.sku, asdict(event))
            else:
                event = gen_supplier_delivery()
                publish(producer, TOPICS["supplier"], event.sku, asdict(event))

        time.sleep(1)

    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    run(events_per_second=10, duration_seconds=120)