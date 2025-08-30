"""
Ads Spend ETL – DuckDB Ingestion Pipeline
-----------------------------------------

This script ingests CSVs from a landing directory into a DuckDB table with
robust cleaning, schema enforcement, and provenance tracking.

Key Features:
    • Environment-driven config:
        - WAREHOUSE_PATH → DuckDB file (default: /data/warehouse/lake.duckdb)
        - LANDING_DIR    → incoming CSV folder (default: /data/landing)
        - ADSSPEND_TABLE → target table name (default: ads_spend)
    • Idempotent loads:
        - Skips files already loaded by checking source_file_name
        - Creates target table if missing (CREATE IF NOT EXISTS)
    • Data quality & typing:
        - Validates required columns: date, platform, account, campaign, country,
          device, spend, clicks, impressions, conversions
        - Cleans money strings (handles $, commas, spaces, parentheses for negatives)
        - Parses dates; casts numerics with coercion
        - Trims string dimensions
    • Provenance:
        - Adds load_date (UTC) and source_file_name to every row
    • Logging:
        - Prints warehouse & landing paths, file list, per-file results, and totals

Typical Flow:
    1) Drop CSVs (e.g., ads_spend_*.csv) into LANDING_DIR
    2) Run this script
    3) New rows are appended to DuckDB with clean types and provenance


Author: Carlos Contreras

"""


import os, re, uuid, duckdb, pandas as pd
from datetime import datetime
from dotenv import load_dotenv
    
load_dotenv()

WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "/data/warehouse/lake.duckdb")
LANDING_DIR    = os.getenv("LANDING_DIR", "/data/landing")
TABLE          = os.getenv("ADSSPEND_TABLE", "ads_spend")

# ---------- spend cleaning ----------
_keep = re.compile(r"[^0-9.\-]")  

def clean_spend(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    s = _keep.sub("", s)
    if s in ("", "-", "."):
        return None
    try:
        val = float(s)
        return -val if neg and val >= 0 else val
    except ValueError:
        return None

# ---------- table setup ----------
CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  date DATE,
  platform VARCHAR,
  account VARCHAR,
  campaign VARCHAR,
  country VARCHAR,
  device VARCHAR,
  spend DOUBLE,
  clicks BIGINT,
  impressions BIGINT,
  conversions BIGINT,
  load_date TIMESTAMP,
  source_file_name VARCHAR
);
"""

def ensure_table(con):
    con.execute(CREATE_SQL)

def file_already_loaded(con, fname: str) -> bool:
    return con.execute(
        f"SELECT 1 FROM {TABLE} WHERE source_file_name = ? LIMIT 1", [fname]
    ).fetchone() is not None

def load_csv_to_df(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=True)
    df.columns = [c.strip() for c in df.columns]

    # required columns
    req = ["date","platform","account","campaign","country","device",
           "spend","clicks","impressions","conversions"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {os.path.basename(path)}: {missing}")

    # Coerce types
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["spend"] = df["spend"].apply(clean_spend)
    for col in ["clicks","impressions","conversions"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Trim strings
    for col in ["platform","account","campaign","country","device"]:
        df[col] = df[col].astype("string").str.strip()

    return df

def main():
    print(f"== ETL start {datetime.utcnow().isoformat()}Z ==")
    print(f"Warehouse: {WAREHOUSE_PATH}")
    print(f"Landing:   {LANDING_DIR}")
    con = duckdb.connect(WAREHOUSE_PATH)
    ensure_table(con)

    files = sorted([f for f in os.listdir(LANDING_DIR) if f.lower().endswith(".csv")])
    print(f"Found {len(files)} CSV(s): {files}")

    if not files:
        print("No CSVs in landing.")
        con.close()
        return

    total = 0
    for fname in files:
        if file_already_loaded(con, fname):
            print(f"SKIP (already loaded): {fname}")
            continue

        path = os.path.join(LANDING_DIR, fname)
        try:
            df = load_csv_to_df(path)
        except Exception as e:
            print(f"ERROR reading '{fname}': {e}")
            continue

        # add provenance
        df["load_date"] = pd.Timestamp.utcnow()
        df["source_file_name"] = fname

        # order columns
        df = df[["date","platform","account","campaign","country","device",
                 "spend","clicks","impressions","conversions","load_date","source_file_name"]]

        # insert
        con.register("df_stage", df)
        con.execute(f"INSERT INTO {TABLE} SELECT * FROM df_stage")
        con.unregister("df_stage")

        rows = len(df)
        total += rows
        print(f"INSERTED {rows} rows from {fname}")

    con.close()
    print(f"== ETL done. Inserted {total} row(s). ==")

if __name__ == "__main__":
    main()

