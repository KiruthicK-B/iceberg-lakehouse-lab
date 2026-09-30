import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from fastapi import FastAPI

app = FastAPI(title="Mock Shopify Store API")
fake = Faker()

PRODUCT_TYPES = ["Apparel", "Accessories", "Beauty", "Home Goods", "Footwear"]


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


USER_POOL = [
    {
        "cust_id": 500000 + i,
        "cust_email": fake.email(),
        "display_name": fake.name(),
        "created_at": _iso(datetime.now(timezone.utc) - timedelta(days=random.randint(0, 900))),
        "country_code": random.choice(["US", "GB", "CA", "AU"]),
    }
    for i in range(150)
]

ITEM_POOL = [
    {
        "sku": f"SKU-{fake.bothify('??####')}",
        "product_title": fake.catch_phrase(),
        "product_type": random.choice(PRODUCT_TYPES),
        "price": round(random.uniform(8, 300), 2),
        "vendor": fake.company(),
    }
    for _ in range(80)
]


@app.get("/users")
def get_users(n: int = 150):
    return USER_POOL[: max(1, min(n, len(USER_POOL)))]


@app.get("/items")
def get_items(n: int = 80):
    return ITEM_POOL[: max(1, min(n, len(ITEM_POOL)))]


@app.get("/orders")
def get_orders(n: int = 200):
    n = max(1, min(n, 5000))
    now = datetime.now(timezone.utc)
    orders = []
    for _ in range(n):
        user = random.choice(USER_POOL)
        item = random.choice(ITEM_POOL)
        qty = random.randint(1, 4)
        orders.append(
            {
                "shopify_order_id": f"SHOP-{fake.bothify('#######')}",
                "cust_id": user["cust_id"],
                "sku": item["sku"],
                "created_at": _iso(now - timedelta(days=random.randint(0, 120))),
                "line_qty": qty,
                "total_price": round(item["price"] * qty, 2),
                "financial_status": random.choice(["paid", "refunded", "pending", "voided"]),
            }
        )
    return orders


@app.get("/health")
def health():
    return {"status": "ok"}
