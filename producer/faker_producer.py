import json
import time
import random
import logging
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

TOPICS = {
    "orders":      "ecom.orders",
    "payments":    "ecom.payments",
    "customers":   "ecom.customers",
    "inventory":   "ecom.inventory",
    "clickstream": "ecom.clickstream",
}

PRODUCT_CATALOG = [
    {"product_id": f"PROD-{str(i).zfill(4)}", "name": fake.catch_phrase(), "category": random.choice(
        ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Beauty", "Automotive"]
    ), "price": round(random.uniform(5.99, 999.99), 2)}
    for i in range(1, 101)
]

CUSTOMER_IDS = [f"CUST-{str(i).zfill(6)}" for i in range(1, 1001)]

ORDER_STATUSES   = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_METHODS  = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
PAYMENT_STATUSES = ["pending", "authorized", "captured", "failed", "refunded", "disputed"]
PAGE_TYPES       = ["home", "product", "category", "cart", "checkout", "search", "account", "wishlist"]
DEVICES          = ["desktop", "mobile", "tablet"]
BROWSERS         = ["Chrome", "Firefox", "Safari", "Edge"]


# ── Schema generators ──────────────────────────────────────────────────────────

def generate_customer():
    customer_id = random.choice(CUSTOMER_IDS)
    return {
        "customer_id":    customer_id,
        "email":          fake.email(),
        "first_name":     fake.first_name(),
        "last_name":      fake.last_name(),
        "phone":          fake.phone_number(),
        "date_of_birth":  fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "gender":         random.choice(["M", "F", "Other", "Prefer not to say"]),
        "address": {
            "street":     fake.street_address(),
            "city":       fake.city(),
            "state":      fake.state(),
            "zip_code":   fake.zipcode(),
            "country":    fake.country_code(),
        },
        "segment":        random.choice(["bronze", "silver", "gold", "platinum"]),
        "signup_date":    fake.date_between(start_date="-5y", end_date="today").isoformat(),
        "is_active":      random.choice([True, True, True, False]),
        "event_time":     datetime.now(timezone.utc).isoformat(),
    }


def generate_order():
    customer_id  = random.choice(CUSTOMER_IDS)
    num_items    = random.randint(1, 6)
    items        = []
    subtotal     = 0.0

    for _ in range(num_items):
        product  = random.choice(PRODUCT_CATALOG)
        qty      = random.randint(1, 4)
        discount = round(random.uniform(0, 0.3), 2)
        line_total = round(product["price"] * qty * (1 - discount), 2)
        subtotal  += line_total
        items.append({
            "product_id":   product["product_id"],
            "product_name": product["name"],
            "category":     product["category"],
            "unit_price":   product["price"],
            "quantity":     qty,
            "discount":     discount,
            "line_total":   line_total,
        })

    tax            = round(subtotal * 0.08, 2)
    shipping_cost  = round(random.uniform(0, 15.99), 2)
    order_total    = round(subtotal + tax + shipping_cost, 2)

    return {
        "order_id":        f"ORD-{fake.uuid4()[:8].upper()}",
        "customer_id":     customer_id,
        "status":          random.choice(ORDER_STATUSES),
        "items":           items,
        "item_count":      num_items,
        "subtotal":        round(subtotal, 2),
        "tax":             tax,
        "shipping_cost":   shipping_cost,
        "order_total":     order_total,
        "currency":        "USD",
        "shipping_address": {
            "street":      fake.street_address(),
            "city":        fake.city(),
            "state":       fake.state(),
            "zip_code":    fake.zipcode(),
            "country":     fake.country_code(),
        },
        "shipping_method": random.choice(["standard", "express", "overnight", "pickup"]),
        "promo_code":      fake.bothify("SAVE##??").upper() if random.random() < 0.2 else None,
        "channel":         random.choice(["web", "mobile_app", "marketplace", "in_store"]),
        "event_time":      datetime.now(timezone.utc).isoformat(),
    }


def generate_payment(order_id=None, order_total=None):
    return {
        "payment_id":        f"PAY-{fake.uuid4()[:8].upper()}",
        "order_id":          order_id or f"ORD-{fake.uuid4()[:8].upper()}",
        "customer_id":       random.choice(CUSTOMER_IDS),
        "payment_method":    random.choice(PAYMENT_METHODS),
        "payment_status":    random.choice(PAYMENT_STATUSES),
        "amount":            order_total or round(random.uniform(10.0, 1500.0), 2),
        "currency":          "USD",
        "transaction_id":    fake.uuid4(),
        "gateway":           random.choice(["stripe", "paypal", "braintree", "adyen", "square"]),
        "gateway_response":  random.choice(["approved", "approved", "approved", "declined", "error"]),
        "card_last_four":    fake.credit_card_number()[-4:] if "card" in random.choice(PAYMENT_METHODS) else None,
        "billing_address": {
            "street":        fake.street_address(),
            "city":          fake.city(),
            "state":         fake.state(),
            "zip_code":      fake.zipcode(),
            "country":       fake.country_code(),
        },
        "failure_reason":    random.choice([None, None, None, "insufficient_funds", "card_expired", "fraud_suspected"]),
        "processing_time_ms": random.randint(120, 3000),
        "event_time":        datetime.now(timezone.utc).isoformat(),
    }


def generate_inventory():
    product = random.choice(PRODUCT_CATALOG)
    stock   = random.randint(0, 500)
    return {
        "inventory_id":      fake.uuid4(),
        "product_id":        product["product_id"],
        "product_name":      product["name"],
        "category":          product["category"],
        "sku":               fake.bothify("SKU-????-####").upper(),
        "warehouse_id":      f"WH-{random.randint(1, 10):02d}",
        "warehouse_location": random.choice(["US-EAST", "US-WEST", "US-CENTRAL", "EU-WEST", "APAC"]),
        "quantity_on_hand":  stock,
        "quantity_reserved": random.randint(0, min(stock, 50)),
        "reorder_point":     random.randint(10, 50),
        "reorder_quantity":  random.randint(50, 200),
        "unit_cost":         round(product["price"] * random.uniform(0.3, 0.6), 2),
        "last_restock_date": fake.date_between(start_date="-90d", end_date="today").isoformat(),
        "supplier_id":       f"SUP-{random.randint(1, 30):03d}",
        "is_active":         stock > 0,
        "event_time":        datetime.now(timezone.utc).isoformat(),
    }


def generate_clickstream():
    session_id = fake.uuid4()
    return {
        "event_id":          fake.uuid4(),
        "session_id":        session_id,
        "customer_id":       random.choice(CUSTOMER_IDS) if random.random() > 0.3 else None,
        "anonymous_id":      fake.uuid4() if random.random() < 0.3 else None,
        "event_type":        random.choice([
            "page_view", "page_view", "page_view",
            "product_view", "product_view",
            "add_to_cart", "remove_from_cart",
            "wishlist_add", "search",
            "checkout_start", "checkout_complete",
            "button_click", "scroll",
        ]),
        "page_type":         random.choice(PAGE_TYPES),
        "page_url":          fake.uri(),
        "referrer_url":      fake.uri() if random.random() > 0.4 else None,
        "product_id":        random.choice(PRODUCT_CATALOG)["product_id"] if random.random() > 0.5 else None,
        "search_query":      fake.words(nb=random.randint(1, 4), ext_word_list=None) if random.random() < 0.2 else None,
        "device_type":       random.choice(DEVICES),
        "browser":           random.choice(BROWSERS),
        "os":                random.choice(["Windows", "macOS", "iOS", "Android", "Linux"]),
        "ip_address":        fake.ipv4_public(),
        "country":           fake.country_code(),
        "city":              fake.city(),
        "time_on_page_sec":  random.randint(1, 600),
        "scroll_depth_pct":  random.randint(0, 100),
        "utm_source":        random.choice([None, "google", "facebook", "email", "organic", "direct"]),
        "utm_medium":        random.choice([None, "cpc", "social", "email", "organic"]),
        "utm_campaign":      fake.slug() if random.random() < 0.3 else None,
        "event_time":        datetime.now(timezone.utc).isoformat(),
    }


# ── Producer setup ─────────────────────────────────────────────────────────────

def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=10,
                batch_size=16384,
                compression_type="gzip",
            )
            logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{retries} failed: {e}. Retrying in {delay}s…")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple attempts.")


def send(producer: KafkaProducer, topic: str, key: str, record: dict):
    producer.send(topic, key=key, value=record).add_errback(
        lambda exc: logger.error(f"Failed to send to {topic}: {exc}")
    )


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    producer = create_producer()
    logger.info("Starting event production loop…")

    messages_sent = 0
    try:
        while True:
            # --- customers (low frequency: ~10% of iterations)
            if random.random() < 0.10:
                customer = generate_customer()
                send(producer, TOPICS["customers"], customer["customer_id"], customer)

            # --- inventory updates (~15% of iterations)
            if random.random() < 0.15:
                inv = generate_inventory()
                send(producer, TOPICS["inventory"], inv["product_id"], inv)

            # --- orders (always)
            order = generate_order()
            send(producer, TOPICS["orders"], order["order_id"], order)

            # --- payment linked to order (~90% have a payment)
            if random.random() < 0.90:
                payment = generate_payment(
                    order_id=order["order_id"],
                    order_total=order["order_total"]
                )
                send(producer, TOPICS["payments"], payment["payment_id"], payment)

            # --- clickstream burst (1-5 events per iteration)
            for _ in range(random.randint(1, 5)):
                click = generate_clickstream()
                send(producer, TOPICS["clickstream"], click["event_id"], click)

            messages_sent += 1
            if messages_sent % 100 == 0:
                producer.flush()
                logger.info(f"Checkpoint: {messages_sent} iterations completed.")

            # ~2-4 events/second
            time.sleep(random.uniform(0.25, 0.5))

    except KeyboardInterrupt:
        logger.info("Shutting down producer…")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed cleanly.")


if __name__ == "__main__":
    main()