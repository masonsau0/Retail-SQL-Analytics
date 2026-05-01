"""Streamlit dashboard for the Retail SQL Analytics queries.

Renders each of 10 PostgreSQL analytical queries side-by-side with its
SQL, result table, and a contextual chart (Pareto, cohort heatmap, RFM
distribution, etc.).

Connects to whichever data backend is available:
    - PostgreSQL via the docker-composed `retail` container, if reachable
    - DuckDB embedded in-memory as a zero-install fallback (used by the
      hosted Streamlit Cloud demo since Cloud cannot run Docker)

Run with::

    docker compose up -d                  # optional — enables the PG path
    pip install -r requirements.txt
    streamlit run retail_analytics_app.py
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DB_DSN = "host=localhost port=5432 dbname=retail user=analyst password=analyst"
QUERIES_FILE = Path(__file__).parent / "queries.sql"
SEED_FILE = Path(__file__).parent / "init" / "02_seed.sql"

st.set_page_config(page_title="Retail SQL Analytics", layout="wide", page_icon="🛒")


# DuckDB-equivalent of init/01_schema.sql. SERIAL is replaced by sequences
# with nextval() defaults so the existing 02_seed.sql (which omits the PK
# columns and relies on auto-assignment) loads as-is. Foreign keys are
# omitted — the seed data is already correctly related, and DuckDB doesn't
# enforce FK constraints during INSERT anyway.
DUCKDB_SCHEMA = """
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS customers;

DROP SEQUENCE IF EXISTS seq_customers;
DROP SEQUENCE IF EXISTS seq_categories;
DROP SEQUENCE IF EXISTS seq_products;
DROP SEQUENCE IF EXISTS seq_orders;
DROP SEQUENCE IF EXISTS seq_order_items;

CREATE SEQUENCE seq_customers START 1;
CREATE SEQUENCE seq_categories START 1;
CREATE SEQUENCE seq_products START 1;
CREATE SEQUENCE seq_orders START 1;
CREATE SEQUENCE seq_order_items START 1;

CREATE TABLE customers (
    customer_id      INTEGER PRIMARY KEY DEFAULT nextval('seq_customers'),
    email            TEXT UNIQUE NOT NULL,
    full_name        TEXT NOT NULL,
    country          TEXT NOT NULL,
    signup_date      DATE NOT NULL,
    marketing_opt_in BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY DEFAULT nextval('seq_categories'),
    name        TEXT UNIQUE NOT NULL
);

CREATE TABLE products (
    product_id  INTEGER PRIMARY KEY DEFAULT nextval('seq_products'),
    sku         TEXT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    list_price  DECIMAL(10, 2) NOT NULL CHECK (list_price >= 0),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE orders (
    order_id      INTEGER PRIMARY KEY DEFAULT nextval('seq_orders'),
    customer_id   INTEGER NOT NULL,
    order_date    TIMESTAMP NOT NULL,
    status        TEXT NOT NULL
                  CHECK (status IN ('placed','shipped','delivered','cancelled','returned')),
    shipping_cost DECIMAL(10, 2) NOT NULL DEFAULT 0
);

CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY DEFAULT nextval('seq_order_items'),
    order_id      INTEGER NOT NULL,
    product_id    INTEGER NOT NULL,
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    unit_price    DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount      DECIMAL(10, 2) NOT NULL DEFAULT 0 CHECK (discount >= 0)
);
"""


@dataclass
class Query:
    qid: str
    label: str
    question: str
    concepts: str
    sql: str


@dataclass
class DB:
    kind: str         # "postgres" or "duckdb"
    conn: Any
    label: str        # short badge text
    n_customers: int
    n_orders: int


@st.cache_data
def load_queries() -> list[Query]:
    sql_text = QUERIES_FILE.read_text(encoding="utf-8")
    header_re = re.compile(r"^-- (Q\d{2})\s*-\s*(.+?)$", re.MULTILINE)
    matches = list(header_re.finditer(sql_text))
    out: list[Query] = []
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(sql_text)
        block = sql_text[m.start():end]
        comment_lines: list[str] = []
        sql_lines: list[str] = []
        seen_sql = False
        for line in block.splitlines()[1:]:
            stripped = line.strip()
            if seen_sql:
                sql_lines.append(line)
                continue
            if stripped.startswith("--"):
                cleaned = stripped.removeprefix("--").lstrip()
                if cleaned and not all(c == "-" for c in cleaned):
                    comment_lines.append(cleaned)
            elif stripped == "":
                continue
            else:
                seen_sql = True
                sql_lines.append(line)
        while sql_lines and (
            not sql_lines[-1].strip()
            or sql_lines[-1].strip().startswith("--")
        ):
            sql_lines.pop()
        question = concepts = ""
        current: str | None = None
        for cl in comment_lines:
            if cl.startswith("Business question:"):
                current = "question"
                question = cl.removeprefix("Business question:").strip()
            elif cl.startswith("Concepts:"):
                current = "concepts"
                concepts = cl.removeprefix("Concepts:").strip()
            elif current == "question":
                question += " " + cl
            elif current == "concepts":
                concepts += " " + cl
        out.append(Query(
            qid=m.group(1),
            label=m.group(2).strip(),
            question=question.strip(),
            concepts=concepts.strip(),
            sql="\n".join(sql_lines).strip(),
        ))
    return out


def _try_postgres() -> DB | None:
    try:
        import psycopg
    except ImportError:
        return None
    try:
        conn = psycopg.connect(DB_DSN, connect_timeout=2, autocommit=True)
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers")
            n_c = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders")
            n_o = cur.fetchone()[0]
    except Exception:
        conn.close()
        return None
    return DB("postgres", conn, "PostgreSQL (Docker)", n_c, n_o)


def _build_duckdb() -> DB:
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.execute(DUCKDB_SCHEMA)
    seed_sql = SEED_FILE.read_text(encoding="utf-8")
    conn.execute(seed_sql)
    n_c = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    n_o = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    return DB("duckdb", conn, "DuckDB (embedded)", n_c, n_o)


@st.cache_resource(show_spinner="Connecting to database…")
def get_db() -> DB:
    pg = _try_postgres()
    if pg is not None:
        return pg
    return _build_duckdb()


def fetch_df(db: DB, sql: str) -> pd.DataFrame:
    if db.kind == "postgres":
        with db.conn.cursor() as cur:
            cur.execute(sql)
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    return db.conn.execute(sql).df()


@st.cache_data(show_spinner=False)
def run_query(_db_kind: str, qid: str, sql: str) -> tuple[pd.DataFrame, float]:
    db = get_db()
    t0 = time.perf_counter()
    df = fetch_df(db, sql)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return df, elapsed_ms


def render_chart(qid: str, df: pd.DataFrame) -> None:
    if qid == "Q01":
        plot_df = df.sort_values("lifetime_revenue")
        fig = px.bar(
            plot_df, x="lifetime_revenue", y="full_name", orientation="h",
            labels={"lifetime_revenue": "Lifetime revenue ($)", "full_name": "Customer"},
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

    elif qid == "Q02":
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df["month"], y=df["monthly_revenue"], name="Monthly"))
        fig.add_trace(go.Scatter(
            x=df["month"], y=df["running_total"], name="Cumulative",
            yaxis="y2", mode="lines+markers",
            line=dict(color="firebrick", width=2),
        ))
        fig.update_layout(
            yaxis=dict(title="Monthly revenue ($)"),
            yaxis2=dict(title="Running total ($)", overlaying="y", side="right"),
            height=420, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, width="stretch")

    elif qid == "Q03":
        plot_df = df.sort_values("revenue", ascending=True)
        fig = px.bar(
            plot_df, x="revenue", y="category_name", orientation="h",
            color="product_name",
            labels={
                "revenue": "Revenue ($)",
                "category_name": "Category",
                "product_name": "Bestseller",
            },
        )
        fig.update_layout(
            height=420, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="v", y=1, x=1.02),
        )
        st.plotly_chart(fig, width="stretch")

    elif qid == "Q04":
        cols = ["m0", "m1", "m3", "m6", "m12"]
        heat = df.set_index("cohort_month")[cols]
        heat.index = pd.to_datetime(heat.index).strftime("%Y-%m")
        fig = px.imshow(
            heat, aspect="auto", color_continuous_scale="Blues", text_auto=True,
            labels=dict(x="Months since signup", y="Cohort", color="Customers"),
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

    elif qid == "Q06":
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["customer_rank"], y=df["revenue"], name="Customer revenue",
        ))
        fig.add_trace(go.Scatter(
            x=df["customer_rank"], y=df["cumulative_pct"],
            name="Cumulative %", yaxis="y2", mode="lines",
            line=dict(color="firebrick", width=2),
        ))
        fig.add_hline(
            y=80, line_dash="dot", line_color="grey",
            annotation_text="80% line", yref="y2",
        )
        fig.update_layout(
            xaxis=dict(title="Customer rank (1 = highest spend)"),
            yaxis=dict(title="Revenue ($)"),
            yaxis2=dict(title="Cumulative %", overlaying="y", side="right",
                        range=[0, 105]),
            height=420, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, width="stretch")

    elif qid == "Q07":
        fig = px.bar(
            df, x="bucket", y="avg_basket", error_y="stddev_basket",
            labels={"avg_basket": "Avg basket ($)", "bucket": ""},
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

    elif qid == "Q08":
        plot_df = df.melt(
            id_vars=["month"],
            value_vars=["delivered", "cancelled", "returned"],
            var_name="status", value_name="orders",
        )
        fig = px.bar(
            plot_df, x="month", y="orders", color="status",
            labels={"orders": "Order count"},
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

    elif qid == "Q09":
        seg_counts = df["segment"].value_counts().reset_index()
        seg_counts.columns = ["segment", "count"]
        fig = px.bar(
            seg_counts, x="segment", y="count", color="segment",
            labels={"count": "Customer count", "segment": "RFM segment"},
        )
        fig.update_layout(
            height=420, margin=dict(l=0, r=0, t=10, b=0), showlegend=False,
        )
        st.plotly_chart(fig, width="stretch")


# --- UI ----------------------------------------------------------------------

st.title("Retail SQL Analytics")
st.caption(
    "Interactive viewer for 10 PostgreSQL analytical queries — window "
    "functions, CTEs, cohort retention, Pareto, and RFM segmentation against "
    "a synthetic retail schema (8 categories · 55 products · 150 customers · "
    "2,500 orders · 5,246 order items)."
)

with st.expander("How to use this app", expanded=False):
    st.markdown("""
**What this app does in plain English.**
A retail company runs an online store: customers, products, orders, and
the line items inside each order. This app shows ten SQL queries that
answer real business questions an analyst gets every week — *who are
our top customers? what's the revenue trend? which customers haven't
ordered in a while? are a few buyers driving most of our revenue?* —
running them live against a synthetic dataset (150 customers, 2,500
orders, 5,246 line items spanning 2024–2026).

**Quick start (30 seconds).**
1. Pick a query in the left sidebar (Q01 through Q10).
2. Read the **Business question** and **SQL concepts** at the top.
3. Expand **View SQL** to see the exact query that ran.
4. Scroll down for the **Result** table and the **Visualisation** chart.

**What you'll see per query.**
- **Business question** — what an analyst is trying to answer.
- **SQL concepts** — the techniques used (window functions, `FILTER`
  clause, `NTILE`, cohort math, anti-joins). Useful if you're brushing
  up on SQL.
- **View SQL** — collapsible panel with the exact query.
- **Rows / Columns / Elapsed** — how many rows came back and how long
  the query took.
- **Result** — sortable table of returned rows.
- **Visualisation** — a chart for the queries where one helps: Pareto
  curve, cohort heatmap, monthly revenue dual-axis, RFM segment
  distribution, and so on.

**The 10 queries at a glance.**
- **Q01–Q03** — top-N analysis: top customers, monthly revenue with
  running total, bestseller per category.
- **Q04** — cohort retention: how many customers from each signup month
  are still ordering N months later.
- **Q05** — lapsed customers: who hasn't ordered in 90 days (anti-join
  with `NOT EXISTS`).
- **Q06** — Pareto analysis: what % of customers drive 80 % of revenue?
- **Q07** — first-order vs subsequent-order basket size.
- **Q08** — monthly order-status mix (delivered / cancelled / returned).
- **Q09** — RFM segmentation: bucket every customer on Recency /
  Frequency / Monetary into a 5×5×5 grid using `NTILE(5)`.
- **Q10** — underperforming products: active SKUs with no revenue in
  the last 6 months (`LEFT JOIN … IS NULL`).

**Try this.** Open **Q06** — the Pareto curve's cumulative-% line crosses
80 % well before customer rank reaches 80 %, the classic revenue
concentration pattern. Then jump to **Q09** and notice how few customers
fall in the **Champions** segment (R = 1, F = 1, M = 1) — those are the
small group of buyers driving most of the spend you just saw in Q06.
""")

db = get_db()
badge = "🐘" if db.kind == "postgres" else "🦆"
st.success(
    f"{badge} **{db.label}** · {db.n_customers} customers · {db.n_orders} orders"
)
if db.kind == "duckdb":
    st.caption(
        "Running on the embedded DuckDB fallback. The queries below use "
        "standard PostgreSQL syntax (window functions, CTEs, FILTER, NTILE, "
        "DATE_TRUNC, INTERVAL, AGE) which DuckDB executes natively. To run "
        "them against actual PostgreSQL instead, `docker compose up -d` "
        "from the project root."
    )

queries = load_queries()
options = [f"{q.qid} — {q.label}" for q in queries]

st.sidebar.markdown("### Pick a query")
sel = st.sidebar.radio(
    "Pick a query", options, index=0, label_visibility="collapsed",
)
q = queries[options.index(sel)]

st.header(f"{q.qid} — {q.label}")
if q.question:
    st.markdown(f"**Business question:** {q.question}")
if q.concepts:
    st.markdown(f"**SQL concepts:** {q.concepts}")

with st.expander("View SQL", expanded=False):
    st.code(q.sql, language="sql")

try:
    df, elapsed_ms = run_query(db.kind, q.qid, q.sql)
except Exception as e:
    st.error(f"Query failed: {e}")
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Rows", len(df))
c2.metric("Columns", len(df.columns))
c3.metric("Elapsed", f"{elapsed_ms:.1f} ms")

st.subheader("Result")
st.dataframe(df, width="stretch", hide_index=True)

CHART_QIDS = {"Q01", "Q02", "Q03", "Q04", "Q06", "Q07", "Q08", "Q09"}
if q.qid in CHART_QIDS:
    st.subheader("Visualisation")
    render_chart(q.qid, df)
