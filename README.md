# Retail SQL Analytics

A **PostgreSQL analytics playground** built around a synthetic retail
schema (customers, products, orders, order items, categories) with **10
analytical queries** that demonstrate window functions, CTEs, anti-joins,
date arithmetic, and standard analytics patterns (Pareto, RFM, cohort
retention).

**Schema scale:** 8 categories · 55 products · 150 customers · 2,500
orders · 5,246 order items spanning Jan 2024 – Apr 2026.

The whole thing runs in one command via `docker compose up -d` : Postgres
boots, the schema is created, and the seed data is loaded automatically.

## What this project shows

| SQL concept | Demonstrated in |
|---|---|
| **CTEs** (multi-step `WITH`) | Q02, Q03, Q04, Q06, Q07, Q09, Q10 |
| **Window functions** : `SUM OVER`, `ROW_NUMBER`, `DENSE_RANK`, `NTILE` | Q02, Q03, Q06, Q07, Q09 |
| **Cumulative running totals** | Q02, Q06 |
| **Partitioned ranking** | Q03 (top product per category) |
| **`FILTER` clause** for conditional aggregation | Q04, Q08 |
| **Anti-joins** : `NOT EXISTS`, `LEFT JOIN ... IS NULL` | Q05, Q10 |
| **Date arithmetic** : `DATE_TRUNC`, `INTERVAL`, `AGE()` | Q02, Q04, Q05, Q08, Q10 |
| **Pareto analysis** (cumulative %) | Q06 |
| **RFM segmentation** (Recency/Frequency/Monetary with `NTILE`) | Q09 |
| **Cohort retention** by signup month | Q04 |

## The 10 queries

1. **Top 10 customers by lifetime spend** : basic aggregation across the
   3-table join (`customers` → `orders` → `order_items`).
2. **Monthly revenue with running total** : `DATE_TRUNC` + windowed
   `SUM OVER (ORDER BY month)` for cumulative-to-date.
3. **Top product per category** : `DENSE_RANK() OVER (PARTITION BY
   category)` then filter to rank = 1.
4. **Customer cohort retention** : bucket customers by signup month, then
   count distinct returners at M+0 / M+1 / M+3 / M+6 / M+12 using
   `FILTER (WHERE months_since_signup = N)`.
5. **Lapsed customers** (no order in last 90 days) : anti-join with
   `NOT EXISTS` against an order-history sub-window.
6. **Pareto analysis** : cumulative revenue % by customer rank, showing
   how concentrated revenue is.
7. **First-order vs subsequent-order basket size** : `ROW_NUMBER` per
   customer to label the first order, then group-by-bucket comparison.
8. **Monthly order-status mix** : delivery rate / cancellation rate using
   the `FILTER` clause for conditional counts.
9. **RFM customer segmentation** : `NTILE(5)` bucketing on Recency,
   Frequency, Monetary, plus a `CASE`-driven segment label.
10. **Underperforming products** : active SKUs with zero revenue in last
    6 months via `LEFT JOIN ... COALESCE(... ,0) = 0`.

## Run it

### Spin up the database

```bash
docker compose up -d
```

Postgres will boot, run `init/01_schema.sql` (DDL), and load
`init/02_seed.sql` (~5,000 row inserts). Takes ~10 seconds on first
startup; subsequent restarts skip init because the data volume persists.

Confirm it's ready:

```bash
docker compose exec postgres pg_isready -U analyst -d retail
```

### Run a single query interactively

```bash
docker compose exec postgres psql -U analyst -d retail
```

Inside `psql`, paste any of the queries from `queries.sql`.

### Run all 10 queries with formatted output

```bash
pip install -r requirements.txt
python run_queries.py
```

This connects via `psycopg`, splits `queries.sql` on the `-- Q<NN>`
markers, runs each query, and prints the result as a Markdown table with
elapsed time per query.

### Tear down

```bash
docker compose down            # stops the container, keeps the data
docker compose down -v         # also drops the data volume
```

## Repository layout

```
.
├── docker-compose.yml          ← Postgres 16-alpine + auto init
├── init/
│   ├── 01_schema.sql           ← CREATE TABLE statements + indexes
│   └── 02_seed.sql             ← auto-generated INSERT statements
├── generate_data.py            ← regenerates init/02_seed.sql
├── queries.sql                 ← the 10 analytical queries
├── run_queries.py              ← Python runner that executes all queries
├── requirements.txt            ← psycopg, tabulate
├── LICENSE
└── README.md
```

## Regenerating the seed data

The seed-data SQL is checked in so the project just runs. To regenerate
with a different scale or seed:

```bash
python generate_data.py        # rewrites init/02_seed.sql
docker compose down -v         # drop the existing data volume
docker compose up -d           # boots fresh, re-runs init
```

Edit constants at the top of `generate_data.py` (`N_CUSTOMERS`,
`N_ORDERS`, the product list, etc.) to change the scale.

## Stack

**PostgreSQL 16** · **Docker / docker-compose** · **psycopg 3**
(Python driver) · **tabulate** (formatted output)
