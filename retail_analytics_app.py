"""Streamlit dashboard for the Retail SQL Analytics queries.

Connects to the docker-composed Postgres, runs each of the 10 analytical
queries on demand, and renders results as interactive tables plus
contextual charts (monthly revenue trend, cohort heatmap, Pareto curve,
RFM segment distribution, etc.).

Run with::

    docker compose up -d
    pip install -r requirements.txt
    streamlit run retail_analytics_app.py
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg
import streamlit as st

DB_DSN = "host=localhost port=5432 dbname=retail user=analyst password=analyst"
QUERIES_FILE = Path(__file__).parent / "queries.sql"

st.set_page_config(page_title="Retail SQL Analytics", layout="wide", page_icon="🛒")


@dataclass
class Query:
    qid: str
    label: str
    question: str
    concepts: str
    sql: str


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
        # Trim trailing blank lines and the next query's leading comment divider
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


@st.cache_resource
def get_connection(dsn: str):
    return psycopg.connect(dsn, autocommit=True)


def fetch_df(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(show_spinner=False)
def run_query(qid: str, sql: str) -> tuple[pd.DataFrame, float]:
    conn = get_connection(DB_DSN)
    t0 = time.perf_counter()
    df = fetch_df(conn, sql)
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
        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig, use_container_width=True)

    elif qid == "Q04":
        cols = ["m0", "m1", "m3", "m6", "m12"]
        heat = df.set_index("cohort_month")[cols]
        heat.index = pd.to_datetime(heat.index).strftime("%Y-%m")
        fig = px.imshow(
            heat, aspect="auto", color_continuous_scale="Blues", text_auto=True,
            labels=dict(x="Months since signup", y="Cohort", color="Customers"),
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig, use_container_width=True)

    elif qid == "Q07":
        fig = px.bar(
            df, x="bucket", y="avg_basket", error_y="stddev_basket",
            labels={"avg_basket": "Avg basket ($)", "bucket": ""},
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig, use_container_width=True)


# --- UI ----------------------------------------------------------------------

st.title("Retail SQL Analytics")
st.caption(
    "Interactive viewer for 10 PostgreSQL analytical queries — window "
    "functions, CTEs, cohort retention, Pareto, and RFM segmentation against "
    "a synthetic retail schema (8 categories · 55 products · 150 customers · "
    "2,500 orders · 5,246 order items)."
)

try:
    conn = get_connection(DB_DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        n_customers = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM orders")
        n_orders = cur.fetchone()[0]
    st.success(
        f"Connected to Postgres · {n_customers} customers, {n_orders} orders "
        f"in `retail`"
    )
except psycopg.OperationalError as e:
    st.error(
        f"Could not connect to Postgres at `{DB_DSN}`.\n\n"
        f"**Fix:** run `docker compose up -d` from the project root, then "
        f"refresh this page.\n\n```\n{e}\n```"
    )
    st.stop()

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

df, elapsed_ms = run_query(q.qid, q.sql)

c1, c2, c3 = st.columns(3)
c1.metric("Rows", len(df))
c2.metric("Columns", len(df.columns))
c3.metric("Elapsed", f"{elapsed_ms:.1f} ms")

st.subheader("Result")
st.dataframe(df, use_container_width=True, hide_index=True)

CHART_QIDS = {"Q01", "Q02", "Q03", "Q04", "Q06", "Q07", "Q08", "Q09"}
if q.qid in CHART_QIDS:
    st.subheader("Visualisation")
    render_chart(q.qid, df)
