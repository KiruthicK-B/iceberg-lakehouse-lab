import random
import time
from datetime import timedelta

from faker import Faker
from fastapi import FastAPI

app = FastAPI(title="Mock Amazon Seller API")
fake = Faker()

CATEGORIES = ["Electronics", "Home & Kitchen", "Books", "Toys", "Sports"]

# Fixed-size in-memory pools generated once at startup, so orders reference
# real, joinable customer_id / asin values instead of floating random IDs.
USER_POOL = [
    {
        "customer_id": 100000 + i,
        "full_name": fake.name(),
        "email_address": fake.email(),
        "signup_epoch": int(time.time()) - random.randint(0, 3 * 365 * 86400),
        "country": "US",
    }
    for i in range(150)
]

ITEM_POOL = [
    {
        "asin": f"B0{fake.bothify('???####??').upper()}",
        "title": fake.catch_phrase(),
        "category": random.choice(CATEGORIES),
        "price_usd": round(random.uniform(5, 500), 2),
        "brand": fake.company(),
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
    now_epoch = int(time.time())
    orders = []
    for _ in range(n):
        user = random.choice(USER_POOL)
        item = random.choice(ITEM_POOL)
        qty = random.randint(1, 5)
        orders.append(
            {
                "order_id": f"AMZ-{fake.bothify('#########')}",
                "customer_id": user["customer_id"],
                "asin": item["asin"],
                "order_epoch": now_epoch - random.randint(0, 120 * 86400),
                "qty": qty,
                "unit_price_usd": item["price_usd"],
                "order_status": random.choice(["shipped", "delivered", "cancelled", "pending"]),
            }
        )
    return orders


@app.get("/health")
def health():
    return {"status": "ok"}
