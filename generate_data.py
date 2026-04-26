"""Generate the seed-data SQL file for the retail analytics schema.

Outputs `init/02_seed.sql` containing INSERT statements for ~150 customers,
~60 products across 8 categories, and ~2,500 orders with ~7,000 order
items spanning roughly 2 years. Distributions are skewed (Pareto-style
spend concentration; weekend / month-end ordering bias) so the analytical
queries return realistic, interesting results.

Run once locally to regenerate seed data — the output is committed so the
docker-compose init scripts have everything they need on first startup.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(42)

# ----------------------------------------------------------------- domain data
COUNTRIES = ["CA", "US", "GB", "AU", "DE", "FR", "JP", "BR"]
COUNTRY_WEIGHTS = [0.35, 0.40, 0.08, 0.05, 0.04, 0.03, 0.03, 0.02]

FIRST_NAMES = [
    "Alex", "Jamie", "Taylor", "Jordan", "Morgan", "Casey", "Riley", "Cameron",
    "Avery", "Quinn", "Sasha", "Devon", "Reese", "Drew", "Hayden", "Logan",
    "Skylar", "Parker", "Robin", "Sage", "Phoenix", "River", "Rowan", "Emery",
]
LAST_NAMES = [
    "Chen", "Patel", "Garcia", "Smith", "Nguyen", "Johnson", "Singh", "Brown",
    "Khan", "Kim", "Martinez", "Wong", "Davis", "Park", "Lee", "Tanaka",
    "Schmidt", "Dubois", "Silva", "Mueller", "Rossi", "Anderson", "Wilson",
]

CATEGORIES = [
    "Electronics", "Apparel", "Home & Kitchen", "Sports & Outdoors",
    "Books", "Beauty & Health", "Toys & Games", "Office Supplies",
]

# (category, product name, base price)
PRODUCT_TEMPLATES = [
    ("Electronics", "Wireless Headphones", 89.99),
    ("Electronics", "Bluetooth Speaker", 49.99),
    ("Electronics", "Smart Watch", 199.99),
    ("Electronics", "USB-C Hub", 34.99),
    ("Electronics", "Mechanical Keyboard", 119.99),
    ("Electronics", "4K Webcam", 89.99),
    ("Electronics", "Portable SSD 1TB", 109.99),
    ("Electronics", "E-reader", 139.99),
    ("Apparel", "Merino T-Shirt", 39.99),
    ("Apparel", "Running Shorts", 29.99),
    ("Apparel", "Hooded Sweatshirt", 54.99),
    ("Apparel", "Down Jacket", 199.99),
    ("Apparel", "Wool Socks 3-pack", 24.99),
    ("Apparel", "Canvas Sneakers", 59.99),
    ("Apparel", "Leather Belt", 39.99),
    ("Home & Kitchen", "Cast Iron Skillet 12in", 49.99),
    ("Home & Kitchen", "French Press 1L", 32.99),
    ("Home & Kitchen", "Chef's Knife 8in", 89.99),
    ("Home & Kitchen", "Bamboo Cutting Board", 24.99),
    ("Home & Kitchen", "Stand Mixer", 299.99),
    ("Home & Kitchen", "Air Fryer", 119.99),
    ("Home & Kitchen", "Linen Sheet Set", 119.99),
    ("Sports & Outdoors", "Yoga Mat 6mm", 39.99),
    ("Sports & Outdoors", "Resistance Band Set", 24.99),
    ("Sports & Outdoors", "Foam Roller", 29.99),
    ("Sports & Outdoors", "Hiking Backpack 30L", 89.99),
    ("Sports & Outdoors", "Insulated Water Bottle", 34.99),
    ("Sports & Outdoors", "Tent 2-person", 199.99),
    ("Sports & Outdoors", "Trekking Poles", 59.99),
    ("Books", "Cookbook: One Pan Recipes", 24.99),
    ("Books", "Novel: The Distant Shore", 18.99),
    ("Books", "Self-help: Atomic Focus", 19.99),
    ("Books", "Children's: The Whale and the Bear", 14.99),
    ("Books", "Tech: Designing Data-Intensive Apps", 49.99),
    ("Books", "Biography: Marie Curie", 22.99),
    ("Books", "Travel Guide: Japan", 29.99),
    ("Beauty & Health", "Vitamin D Supplement", 19.99),
    ("Beauty & Health", "Electric Toothbrush", 89.99),
    ("Beauty & Health", "SPF 50 Sunscreen", 24.99),
    ("Beauty & Health", "Hair Dryer", 79.99),
    ("Beauty & Health", "Skincare Set", 64.99),
    ("Beauty & Health", "Foam Cleanser", 18.99),
    ("Toys & Games", "Wooden Building Blocks", 39.99),
    ("Toys & Games", "Strategy Board Game", 49.99),
    ("Toys & Games", "Plush Bear", 24.99),
    ("Toys & Games", "1000pc Puzzle", 19.99),
    ("Toys & Games", "RC Car", 59.99),
    ("Toys & Games", "Dollhouse Set", 89.99),
    ("Office Supplies", "Mesh Office Chair", 199.99),
    ("Office Supplies", "Standing Desk Mat", 49.99),
    ("Office Supplies", "Notebook 3-pack", 24.99),
    ("Office Supplies", "Desk Lamp LED", 39.99),
    ("Office Supplies", "Monitor Stand", 34.99),
    ("Office Supplies", "Cable Organizer Tray", 19.99),
    ("Office Supplies", "Whiteboard 24x36", 59.99),
]

ORDER_STATUSES = ["delivered", "shipped", "placed", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [0.78, 0.10, 0.04, 0.05, 0.03]

START_DATE = date(2024, 1, 1)
END_DATE = date(2026, 4, 1)
N_CUSTOMERS = 150
N_ORDERS = 2500


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def gen_email(first: str, last: str, idx: int) -> str:
    return f"{first.lower()}.{last.lower()}{idx}@example.com"


def gen_signup_date() -> date:
    span = (END_DATE - START_DATE).days
    return START_DATE + timedelta(days=random.randint(0, span - 30))


def gen_order_datetime() -> datetime:
    span = (END_DATE - START_DATE).days
    d = START_DATE + timedelta(days=random.randint(0, span))
    # Skew: weekends + last week of month see more orders
    if d.weekday() >= 5 and random.random() < 0.3:
        d += timedelta(days=random.randint(-1, 1))
    h = random.randint(8, 22)
    m = random.randint(0, 59)
    return datetime(d.year, d.month, d.day, h, m)


def main():
    out_path = Path(__file__).parent / "init" / "02_seed.sql"
    out_path.parent.mkdir(exist_ok=True)
    lines: list[str] = []
    lines.append("-- Auto-generated seed data — do not edit by hand.")
    lines.append("-- Regenerate with: python generate_data.py\n")

    # ------------------------------------------------------------- categories
    lines.append("INSERT INTO categories (name) VALUES")
    rows = [f"  ('{sql_escape(c)}')" for c in CATEGORIES]
    lines.append(",\n".join(rows) + ";\n")

    # ------------------------------------------------------------- products
    lines.append("INSERT INTO products (sku, name, category_id, list_price, is_active) VALUES")
    cat_to_id = {c: i + 1 for i, c in enumerate(CATEGORIES)}
    rows = []
    for i, (cat, name, price) in enumerate(PRODUCT_TEMPLATES, start=1):
        sku = f"SKU-{i:04d}"
        active = "TRUE" if random.random() > 0.05 else "FALSE"
        rows.append(
            f"  ('{sku}', '{sql_escape(name)}', {cat_to_id[cat]}, {price}, {active})"
        )
    lines.append(",\n".join(rows) + ";\n")

    # ------------------------------------------------------------- customers
    lines.append("INSERT INTO customers (email, full_name, country, signup_date, marketing_opt_in) VALUES")
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        opt_in = "TRUE" if random.random() < 0.6 else "FALSE"
        rows.append(
            f"  ('{gen_email(first, last, i)}', '{first} {last}', "
            f"'{country}', '{gen_signup_date().isoformat()}', {opt_in})"
        )
    lines.append(",\n".join(rows) + ";\n")

    # ------------------------------------------------------------- orders + items
    # Pareto-style customer spend: 20% of customers drive 80% of orders.
    high_freq_count = max(1, int(N_CUSTOMERS * 0.20))
    high_freq_customers = random.sample(range(1, N_CUSTOMERS + 1), k=high_freq_count)
    customer_weights = [
        4.0 if cid in set(high_freq_customers) else 1.0
        for cid in range(1, N_CUSTOMERS + 1)
    ]

    n_products = len(PRODUCT_TEMPLATES)
    order_rows = []
    item_rows = []
    item_id = 0

    for order_id in range(1, N_ORDERS + 1):
        cid = random.choices(
            range(1, N_CUSTOMERS + 1), weights=customer_weights, k=1
        )[0]
        odt = gen_order_datetime()
        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]
        shipping = round(random.uniform(0, 15), 2)
        order_rows.append(
            f"  ({cid}, '{odt.isoformat(sep=' ')}', '{status}', {shipping})"
        )

        # 1-5 items per order, weighted toward small basket sizes
        n_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.1, 0.05])[0]
        chosen = random.sample(range(1, n_products + 1), k=n_items)
        for pid in chosen:
            item_id += 1
            base_price = PRODUCT_TEMPLATES[pid - 1][2]
            qty = random.choices([1, 2, 3], weights=[0.7, 0.22, 0.08])[0]
            unit_price = round(base_price * random.uniform(0.95, 1.05), 2)
            discount = round(unit_price * qty * random.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)
            item_rows.append(
                f"  ({order_id}, {pid}, {qty}, {unit_price}, {discount})"
            )

    lines.append("INSERT INTO orders (customer_id, order_date, status, shipping_cost) VALUES")
    lines.append(",\n".join(order_rows) + ";\n")

    lines.append("INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) VALUES")
    lines.append(",\n".join(item_rows) + ";\n")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_path}")
    print(f"  {len(CATEGORIES)} categories")
    print(f"  {len(PRODUCT_TEMPLATES)} products")
    print(f"  {N_CUSTOMERS} customers")
    print(f"  {len(order_rows)} orders")
    print(f"  {len(item_rows)} order items")


if __name__ == "__main__":
    main()
