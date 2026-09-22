"""
producer.py
-----------
Generates realistic fake e-commerce events and publishes them to Redpanda
(Kafka-compatible) at a configurable rate.

Topics:
  ecommerce.orders    — ~25% of events
  ecommerce.payments  — ~15% of events
  ecommerce.clicks    — ~60% of events

Run:
  python scripts/producer.py
  python scripts/producer.py --rate 50   # 50 events/sec
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
import os
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = {
    "orders":   "ecommerce.orders",
    "payments": "ecommerce.payments",
    "clicks":   "ecommerce.clicks",
}

DEFAULT_EVENTS_PER_SECOND = 10

# ---------------------------------------------------------------------------
# Static entity pools (reused across events for realistic correlations)
# ---------------------------------------------------------------------------
fake = Faker("pt_BR")   # Brazilian locale — matches e-commerce theme

CATEGORIES = [
    "electronics", "clothing", "books", "home_garden",
    "sports",      "toys",     "beauty", "food_beverage",
    "automotive",  "health",
]

PAYMENT_METHODS = ["credit_card", "debit_card", "boleto", "voucher", "pix"]
ORDER_STATUSES  = ["created", "processing", "shipped", "delivered", "cancelled"]
STATUS_WEIGHTS  = [0.15,      0.25,         0.20,      0.35,        0.05]

CLICK_ACTIONS       = ["view", "add_to_cart", "remove_from_cart", "wishlist", "share"]
CLICK_ACTION_WEIGHTS = [0.60,   0.20,          0.05,               0.10,       0.05]

DEVICES        = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55,     0.35,      0.10]

NUM_CUSTOMERS = 1_000
NUM_PRODUCTS  = 200
NUM_SELLERS   = 50

# Pre-generate entity pools once at startup
CUSTOMER_IDS = [str(uuid.uuid4()) for _ in range(NUM_CUSTOMERS)]
SELLER_IDS   = [str(uuid.uuid4()) for _ in range(NUM_SELLERS)]
PRODUCTS     = [
    {
        "product_id": str(uuid.uuid4()),
        "category":   random.choice(CATEGORIES),
        "base_price": round(random.uniform(10.0, 500.0), 2),
    }
    for _ in range(NUM_PRODUCTS)
]


# ---------------------------------------------------------------------------
# Event Generator
# ---------------------------------------------------------------------------
class EcommerceEventGenerator:
    """Generates correlated e-commerce events using Faker."""

    def __init__(self, producer: Producer) -> None:
        self.producer = producer
        # Rolling buffer of recent orders so payments can reference them
        self._pending_orders: list[dict] = []

    # ── helpers ──────────────────────────────────────────────────────────
    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _register_order(self, order: dict) -> None:
        """Keep a rolling window of the last 500 orders for payment correlation."""
        self._pending_orders.append(order)
        if len(self._pending_orders) > 500:
            self._pending_orders.pop(0)

    # ── event builders ────────────────────────────────────────────────────
    def build_order(self) -> dict:
        product  = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)
        price    = round(product["base_price"] * random.uniform(0.85, 1.15), 2)
        total    = round(price * quantity, 2)

        order = {
            "order_id":         str(uuid.uuid4()),
            "customer_id":      random.choice(CUSTOMER_IDS),
            "product_id":       product["product_id"],
            "seller_id":        random.choice(SELLER_IDS),
            "product_category": product["category"],
            "quantity":         quantity,
            "unit_price":       price,
            "total_amount":     total,
            "order_status":     random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0],
            "payment_method":   random.choice(PAYMENT_METHODS),
            "event_type":       "order",
            "created_at":       self._now(),
        }
        self._register_order(order)
        return order

    def build_payment(self) -> dict:
        if self._pending_orders:
            ref = random.choice(self._pending_orders)
            order_id    = ref["order_id"]
            customer_id = ref["customer_id"]
            amount      = ref["total_amount"]
        else:
            order_id    = str(uuid.uuid4())
            customer_id = random.choice(CUSTOMER_IDS)
            amount      = round(random.uniform(20.0, 1_000.0), 2)

        return {
            "payment_id":   str(uuid.uuid4()),
            "order_id":     order_id,
            "customer_id":  customer_id,
            "payment_type": random.choice(PAYMENT_METHODS),
            "installments": random.choices(
                [1, 2, 3, 6, 12],
                weights=[0.50, 0.20, 0.15, 0.10, 0.05],
            )[0],
            "amount":       amount,
            "status":       random.choices(
                ["approved", "pending", "declined", "refunded"],
                weights=[0.80, 0.10, 0.07, 0.03],
            )[0],
            "event_type":   "payment",
            "created_at":   self._now(),
        }

    def build_click(self) -> dict:
        product = random.choice(PRODUCTS)
        return {
            "click_id":       str(uuid.uuid4()),
            "session_id":     str(uuid.uuid4()),
            "user_id":        random.choice(CUSTOMER_IDS),
            "product_id":     product["product_id"],
            "category":       product["category"],
            "action":         random.choices(CLICK_ACTIONS, weights=CLICK_ACTION_WEIGHTS)[0],
            "device_type":    random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
            "page_url":       f"/product/{product['product_id']}",
            "event_type":     "click",
            "event_timestamp": self._now(),
        }

    # ── publisher ────────────────────────────────────────────────────────
    def publish(self, topic: str, key: str, event: dict) -> None:
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=self._on_delivery,
        )

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            logger.error(f"Delivery failed [{msg.topic()}]: {err}")

    # ── main loop ────────────────────────────────────────────────────────
    def run(self, events_per_second: int = DEFAULT_EVENTS_PER_SECOND) -> None:
        delay = 1.0 / events_per_second
        logger.info(f"🚀  Producer started — target rate: {events_per_second} events/sec")
        logger.info(f"    Entity pools: {NUM_CUSTOMERS} customers, {NUM_PRODUCTS} products, {NUM_SELLERS} sellers")

        count = 0
        try:
            while True:
                kind = random.choices(
                    ["click", "order", "payment"],
                    weights=[0.60, 0.25, 0.15],
                )[0]

                if kind == "order":
                    event = self.build_order()
                    self.publish(TOPICS["orders"], event["order_id"], event)

                elif kind == "payment":
                    event = self.build_payment()
                    self.publish(TOPICS["payments"], event["payment_id"], event)

                else:
                    event = self.build_click()
                    self.publish(TOPICS["clicks"], event["click_id"], event)

                count += 1
                # Poll delivery callbacks every 100 events
                if count % 100 == 0:
                    self.producer.poll(0)
                    logger.info(f"📤  Published {count:,} events total")

                time.sleep(delay)

        except KeyboardInterrupt:
            logger.info("⛔  Keyboard interrupt — shutting down...")
        finally:
            self.producer.flush()
            logger.info(f"✅  Producer stopped. Total events published: {count:,}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Olist e-commerce event producer")
    parser.add_argument(
        "--rate",
        type=int,
        default=DEFAULT_EVENTS_PER_SECOND,
        help="Events per second (default: 10)",
    )
    args = parser.parse_args()

    kafka_conf = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms":          10,
        "batch.size":         16_384,
        "compression.type":   "lz4",
        "acks":               "all",
    }

    producer = Producer(kafka_conf)
    generator = EcommerceEventGenerator(producer)
    generator.run(events_per_second=args.rate)
