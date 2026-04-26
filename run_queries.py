"""Run all queries in queries.sql against the running Postgres container,
print the results, and time each one. Useful for browsing every query
without firing up a SQL client.

Prerequisites:
    docker compose up -d
    pip install -r requirements.txt
    python run_queries.py
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import psycopg
from tabulate import tabulate

DB_DSN = "host=localhost port=5432 dbname=retail user=analyst password=analyst"


def split_queries(sql_text: str) -> list[tuple[str, str]]:
    """Split queries.sql into (header_label, sql_text) pairs.

    Each query in the file starts with a `-- Q<NN> - <label>` line; this
    splitter uses those markers as boundaries.
    """
    pieces = []
    pattern = re.compile(r"^-- (Q\d{2})\s*-\s*(.+?)$", re.MULTILINE)
    matches = list(pattern.finditer(sql_text))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(sql_text)
        block = sql_text[m.start():end]
        # Strip leading comment lines and trailing whitespace
        sql_only_lines = []
        for line in block.splitlines():
            stripped = line.strip()
            if stripped.startswith("-- ") or stripped == "--" or stripped == "" \
                    and not sql_only_lines:
                continue
            sql_only_lines.append(line)
        sql_only = "\n".join(sql_only_lines).strip()
        pieces.append((f"{m.group(1)} - {m.group(2).strip()}", sql_only))
    return pieces


def main() -> int:
    sql_path = Path(__file__).parent / "queries.sql"
    sql_text = sql_path.read_text(encoding="utf-8")
    queries = split_queries(sql_text)

    print(f"Found {len(queries)} queries in {sql_path.name}\n")

    try:
        conn = psycopg.connect(DB_DSN)
    except psycopg.OperationalError as e:
        print(f"Could not connect to Postgres at {DB_DSN}")
        print(f"  {e}")
        print("  Hint: run `docker compose up -d` first.")
        return 1

    with conn:
        with conn.cursor() as cur:
            for label, sql in queries:
                print("=" * 78)
                print(label)
                print("=" * 78)
                t0 = time.perf_counter()
                cur.execute(sql)
                rows = cur.fetchall()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                cols = [d.name for d in cur.description] if cur.description else []
                if not rows:
                    print("(no rows)")
                else:
                    print(tabulate(rows[:15], headers=cols, tablefmt="github"))
                    if len(rows) > 15:
                        print(f"... and {len(rows) - 15} more rows")
                print(f"\nrows: {len(rows)}  ·  elapsed: {elapsed_ms:.1f} ms\n")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
