import json
import uuid
import random
from datetime import datetime, timedelta, timezone

# -------------------------------------------------
# Master Data
# -------------------------------------------------

STORES = [
    {"store_id": "IN-HYD-001", "region": "APAC", "country": "IN", "city": "Hyderabad", "timezone": "Asia/Kolkata"},
    {"store_id": "IN-BLR-002", "region": "APAC", "country": "IN", "city": "Bangalore", "timezone": "Asia/Kolkata"},
    {"store_id": "IN-MUM-003", "region": "APAC", "country": "IN", "city": "Mumbai", "timezone": "Asia/Kolkata"},
    {"store_id": "IN-DEL-004", "region": "APAC", "country": "IN", "city": "Delhi", "timezone": "Asia/Kolkata"},
    {"store_id": "SG-SIN-001", "region": "APAC", "country": "SG", "city": "Singapore", "timezone": "Asia/Singapore"},
    {"store_id": "JP-TYO-001", "region": "APAC", "country": "JP", "city": "Tokyo", "timezone": "Asia/Tokyo"},

    {"store_id": "US-NYC-001", "region": "NA", "country": "US", "city": "New York", "timezone": "America/New_York"},
    {"store_id": "US-SFO-002", "region": "NA", "country": "US", "city": "San Francisco", "timezone": "America/Los_Angeles"},
    {"store_id": "US-CHI-003", "region": "NA", "country": "US", "city": "Chicago", "timezone": "America/Chicago"},
    {"store_id": "CA-TOR-001", "region": "NA", "country": "CA", "city": "Toronto", "timezone": "America/Toronto"},

    {"store_id": "UK-LON-001", "region": "EU", "country": "UK", "city": "London", "timezone": "Europe/London"},
    {"store_id": "FR-PAR-001", "region": "EU", "country": "FR", "city": "Paris", "timezone": "Europe/Paris"},
    {"store_id": "DE-BER-001", "region": "EU", "country": "DE", "city": "Berlin", "timezone": "Europe/Berlin"},
    {"store_id": "ES-MAD-001", "region": "EU", "country": "ES", "city": "Madrid", "timezone": "Europe/Madrid"},
    {"store_id": "IT-MIL-001", "region": "EU", "country": "IT", "city": "Milan", "timezone": "Europe/Rome"}
]

PRODUCTS = [
    {"sku_id": "SKU-NIKE-001", "brand": "Nike", "price": 2499.00},
    {"sku_id": "SKU-NIKE-002", "brand": "Nike", "price": 2999.00},
    {"sku_id": "SKU-NIKE-003", "brand": "Nike", "price": 2199.00},
    {"sku_id": "SKU-ADIDAS-001", "brand": "Adidas", "price": 1999.00},
    {"sku_id": "SKU-ADIDAS-002", "brand": "Adidas", "price": 2799.00},
    {"sku_id": "SKU-ADIDAS-003", "brand": "Adidas", "price": 2399.00},
    {"sku_id": "SKU-PUMA-001", "brand": "Puma", "price": 1499.00},
    {"sku_id": "SKU-PUMA-002", "brand": "Puma", "price": 1899.00},
    {"sku_id": "SKU-PUMA-003", "brand": "Puma", "price": 2099.00},
    {"sku_id": "SKU-REEBOK-001", "brand": "Reebok", "price": 1699.00},
    {"sku_id": "SKU-REEBOK-002", "brand": "Reebok", "price": 1999.00},
    {"sku_id": "SKU-REEBOK-003", "brand": "Reebok", "price": 2299.00},
    {"sku_id": "SKU-ASICS-001", "brand": "Asics", "price": 2799.00},
    {"sku_id": "SKU-ASICS-002", "brand": "Asics", "price": 3199.00},
    {"sku_id": "SKU-ASICS-003", "brand": "Asics", "price": 2899.00},
    {"sku_id": "SKU-NB-001", "brand": "New Balance", "price": 2599.00},
    {"sku_id": "SKU-NB-002", "brand": "New Balance", "price": 2999.00},
    {"sku_id": "SKU-NB-003", "brand": "New Balance", "price": 3299.00},
    {"sku_id": "SKU-UA-001", "brand": "Under Armour", "price": 2399.00},
    {"sku_id": "SKU-UA-002", "brand": "Under Armour", "price": 2699.00},
    {"sku_id": "SKU-UA-003", "brand": "Under Armour", "price": 2999.00},
    {"sku_id": "SKU-FILA-001", "brand": "Fila", "price": 1799.00},
    {"sku_id": "SKU-FILA-002", "brand": "Fila", "price": 2099.00},
    {"sku_id": "SKU-FILA-003", "brand": "Fila", "price": 2399.00},
    {"sku_id": "SKU-SAUCONY-001", "brand": "Saucony", "price": 3499.00},
    {"sku_id": "SKU-SAUCONY-002", "brand": "Saucony", "price": 3799.00},
    {"sku_id": "SKU-SAUCONY-003", "brand": "Saucony", "price": 3299.00}
]

PAYMENT_METHODS = ["CARD", "UPI", "WALLET"]
PAYMENT_PROVIDERS = ["VISA", "MASTERCARD", "PAYTM", "GPAY", "APPLE_PAY"]
EVENT_TYPES = ["SALE", "REFUND", "PAYMENT_FAILED"]


# Helper Functions
def utc_now():
    return datetime.now(timezone.utc)

def random_event_time():
    return utc_now() - timedelta(minutes=random.randint(0, 5))

def generate_payment_token():
    return f"tok_{uuid.uuid4().hex[:12]}"

def choose_event_type():
    return random.choices(EVENT_TYPES, weights=[0.8, 0.1, 0.1], k=1)[0]


# Event Generator
def generate_pos_event():
    store = random.choice(STORES)
    product = random.choice(PRODUCTS)
    event_type = choose_event_type()

    event_time = random_event_time()
    ingest_time = event_time + timedelta(seconds=random.randint(1, 5))

    transaction_id = f"TXN-{uuid.uuid4().hex[:10]}"
    terminal_id = f"{store['store_id']}-POS-01"

    return {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": event_time.isoformat(),
        "ingest_time": ingest_time.isoformat(),
        "source_system": "POS",
        "store": store,
        "terminal": {
            "terminal_id": terminal_id,
            "software_version": "v3.4.2"
        },
        "transaction": {
            "transaction_id": transaction_id,
            "transaction_type": event_type,
            "transaction_status": "SUCCESS" if event_type != "PAYMENT_FAILED" else "FAILED",
            "transaction_amount": product["price"],
            "currency": "INR" if store["country"] == "IN" else "USD"
        },
        "payment": {
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_provider": random.choice(PAYMENT_PROVIDERS),
            "payment_token": generate_payment_token(),
            "payment_status": "CAPTURED" if event_type == "SALE" else "FAILED"
        },
        "items": [
            {
                "sku_id": product["sku_id"],
                "brand": product["brand"],
                "quantity": 1,
                "total_price": product["price"]
            }
        ],
        "metadata": {
            "channel": "IN_STORE"
        }
    }
