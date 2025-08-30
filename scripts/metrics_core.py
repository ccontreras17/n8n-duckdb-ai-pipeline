"""
Metrics Core – DuckDB KPI Engine
--------------------------------

This module implements the core SQL and computation layer for marketing KPI analysis 
(Customer Acquisition Cost and Return on Ad Spend) on top of a DuckDB warehouse.

Key Features:
    • Anchoring:
        - anchor_date(): finds the latest available data date (<= today).
        - max_data_date(): returns the absolute maximum date in ads_spend.
    • KPI Computation:
        - compute_kpi_compare(): runs a prebuilt SQL file to calculate last 30 days 
          vs prior 30 days KPIs, returning structured JSON.
        - compute_kpi_single(): aggregates KPIs over a custom [start, end] window, 
          with optional grouping (platform, account, campaign, country, device).
    • Validation:
        - _validate_group_by(): ensures group_by dimensions are valid and deduplicated.
    • Formatting:
        - df_to_markdown_table(): converts Pandas DataFrames into GitHub-flavored 
          Markdown tables for easy reporting and AI summarization.

Tech Stack:
    - DuckDB (in-process OLAP engine)
    - Pandas (data handling / formatting)
    - SQL templates (external .sql files, configurable via env vars)

"""

import os
import duckdb
import pandas as pd
from datetime import date
from dotenv import load_dotenv

load_dotenv()

DB_PATH  = os.getenv("WAREHOUSE_PATH", "/data/warehouse/lake.duckdb")
SQL_DIR  = os.getenv("SQL_DIR", "scripts/sql")
KPI_SQL  = os.path.join(SQL_DIR, "kpi_30d_vs_prior.sql")

ALLOWED_GROUP_BY = {"platform", "account", "campaign", "country", "device"}

def _read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def anchor_date(db_path: str = DB_PATH) -> date | None:
    con = duckdb.connect(db_path, read_only=True)
    try:
        row = con.execute('SELECT LEAST(MAX("date"), CURRENT_DATE)::DATE FROM ads_spend').fetchone()
        return row[0] if row else None
    finally:
        con.close()

def max_data_date(db_path: str = DB_PATH) -> date | None:
    con = duckdb.connect(db_path, read_only=True)
    try:
        row = con.execute('SELECT MAX("date")::DATE FROM ads_spend').fetchone()
        return row[0] if row else None
    finally:
        con.close()

def compute_kpi_compare(db_path: str = DB_PATH, sql_path: str = KPI_SQL) -> dict:
    sql = _read_sql(sql_path)
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(sql).fetch_df()
        a = anchor_date(db_path)
    finally:
        con.close()
    return {
        "meta": {"mode": "compare", "anchor_date": str(a) if a else None, "source": db_path},
        "data": df.to_dict(orient="records")
    }

def _validate_group_by(group_by: str | None) -> list[str]:
    if not group_by:
        return []
    requested = [c.strip() for c in group_by.split(",") if c.strip()]
    bad = [c for c in requested if c not in ALLOWED_GROUP_BY]
    if bad:
        raise ValueError(f"Unknown group_by columns: {', '.join(bad)}. Allowed: {', '.join(sorted(ALLOWED_GROUP_BY))}")
    # keep order but dedupe
    seen, clean = set(), []
    for c in requested:
        if c not in seen:
            seen.add(c); clean.append(c)
    return clean

def compute_kpi_single(
    start: str | None,
    end: str | None,
    group_by: str | None = None,
    db_path: str = DB_PATH
) -> dict:
    a = anchor_date(db_path)
    if a is None:
        return {"meta": {"mode": "single", "start": None, "end": None, "source": db_path}, "data": []}

    # Defaults: last 30 days up to anchor
    if not end:
        end = str(a)
    if not start:
        con = duckdb.connect(db_path, read_only=True)
        try:
            
            start_row = con.execute(
                "SELECT (CAST(? AS DATE) - INTERVAL '29 days')::DATE",
                [end]
            ).fetchone()
            start = str(start_row[0])
        finally:
            con.close()

    # Validate order (start <= end)
    con = duckdb.connect(db_path, read_only=True)
    try:
        
        rng_ok = con.execute(
            "SELECT CASE WHEN CAST(? AS DATE) <= CAST(? AS DATE) THEN 1 ELSE 0 END",
            [start, end]
        ).fetchone()[0]
        if not rng_ok:
            raise ValueError("start must be <= end")
    finally:
        con.close()

    group_cols = _validate_group_by(group_by)
    select_dims = ", ".join(group_cols) + (", " if group_cols else "")
    group_clause = ("GROUP BY " + ", ".join(group_cols)) if group_cols else ""

    sql = f"""
    WITH w AS (
      SELECT {select_dims} spend, conversions
      FROM ads_spend
      WHERE "date" BETWEEN ? AND ?
    ),
    agg AS (
      SELECT
        {select_dims}
        SUM(spend)        AS spend,
        SUM(conversions)  AS conversions,
        SUM(conversions)*100.0 AS revenue
      FROM w
      {group_clause}
    ),
    out AS (
      SELECT {select_dims} 'spend' AS metric, spend AS value FROM agg
      UNION ALL
      SELECT {select_dims} 'conversions', conversions FROM agg
      UNION ALL
      SELECT {select_dims} 'revenue', revenue FROM agg
      UNION ALL
      SELECT {select_dims} 'CAC',
        CASE WHEN conversions > 0 THEN spend / conversions END
      FROM agg
      UNION ALL
      SELECT {select_dims} 'ROAS',
        CASE WHEN spend > 0 THEN revenue / spend END
      FROM agg
    )
    SELECT * FROM out
    ORDER BY {(', '.join(group_cols) + ', ') if group_cols else ''} 
      CASE metric
        WHEN 'spend' THEN 1
        WHEN 'conversions' THEN 2
        WHEN 'revenue' THEN 3
        WHEN 'CAC' THEN 4
        WHEN 'ROAS' THEN 5
        ELSE 6
      END;
    """
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(sql, [start, end]).fetch_df()
    finally:
        con.close()

    return {
        "meta": {"mode": "single", "start": start, "end": end, "source": db_path, "group_by": group_cols},
        "data": df.to_dict(orient="records")
    }


def df_to_markdown_table(df: pd.DataFrame) -> str:
    def fmt(cell, col):
        if col.lower() == "pct_change" and pd.notna(cell):
            return f"{cell*100:+.2f}%"
        if isinstance(cell, float):
            return f"{cell:,.2f}"
        return str(cell)
    cols = list(df.columns)
    lines = []
    lines.append('| ' + ' | '.join(cols) + ' |')
    lines.append('| ' + ' | '.join(['---']*len(cols)) + ' |')
    for _, row in df.iterrows():
        lines.append('| ' + ' | '.join(fmt(row[c], c) for c in cols) + ' |')
    return '\n'.join(lines)

