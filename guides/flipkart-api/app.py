import random
from datetime import date, timedelta

from faker import Faker
from fastapi import FastAPI

app = FastAPI(title="Mock Flipkart Seller API")
fake = Faker("en_IN")

CATEGORIES = ["Mobiles", "Fashion", "Appliances", "Grocery", "Beauty"]
STATES = ["Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "West Bengal"]


USER_POOL = [
    {
        "user_id": 900000 + i,
        "name": fake.name(),
        "email": fake.email(),
        "join_date": (date.today() - timedelta(days=random.randint(0, 1000))).isoformat(),
        "state": random.choice(STATES),
    }
    for i in range(150)
]

ITEM_POOL = [
    {
        "product_id": f"FKI-{fake.bothify('??####')}",
        "product_name": fake.catch_phrase(),
        "category": random.choice(CATEGORIES),
        "mrp_inr": round(random.uniform(200, 40000), 2),
        "seller": fake.company(),
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
    orders = []
    for _ in range(n):
        user = random.choice(USER_POOL)
        item = random.choice(ITEM_POOL)
        qty = random.randint(1, 3)
        orders.append(
            {
                "order_no": f"FLP-{fake.bothify('#########')}",
                "user_id": user["user_id"],
                "product_id": item["product_id"],
                "order_date": (date.today() - timedelta(days=random.randint(0, 120))).isoformat(),
                "quantity": qty,
                "amount_inr": round(item["mrp_inr"] * qty, 2),
                "status": random.choice(["delivered", "shipped", "returned", "confirmed"]),
            }
        )
    return orders


@app.get("/health")
def health():
    return {"status": "ok"}
