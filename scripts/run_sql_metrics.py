"""
KPI CLI Tool – DuckDB Marketing Analytics
-----------------------------------------

This script provides a command-line interface (CLI) for querying and reporting 
marketing KPIs (CAC and ROAS) directly from a DuckDB database.

Key Features:
    • Runs prebuilt SQL models from metrics_core against DuckDB.
    • Supports two analysis modes:
        - compare:  Last 30 days vs prior 30 days
        - single:   Aggregate KPIs within a user-specified [start, end] date range
    • Flexible output formats:
        - table (ASCII)
        - json
        - csv (optionally write to file)
    • Optional grouping (e.g., by platform, country) for single mode.
    • Works standalone from the command line or inside scripts/pipelines.

Usage Examples:
    # Compare CAC/ROAS (last30 vs prior30) as a pretty table
    python kpi_cli.py --mode compare

    # Aggregate CAC/ROAS for a custom window, grouped by platform
    python kpi_cli.py --mode single --start 2025-07-01 --end 2025-07-30 --group_by platform

    # Export results to JSON
    python kpi_cli.py --mode compare --format json --out results.json

Tech Stack:
    - DuckDB (analytics warehouse)
    - Pandas (data manipulation)
    - argparse (command-line interface)


"""

import os
import sys
import argparse
import pandas as pd
from metrics_core import compute_kpi_compare, compute_kpi_single, DB_PATH

def to_pretty_table(df: pd.DataFrame) -> str:
    def fmt_val(x, col):
        if pd.isna(x): return ""
        if col == "pct_change": return f"{x*100:+.2f}%"
        if isinstance(x, (int, float)):
            return f"{x:,.4f}" if abs(x) < 1000 else f"{x:,.2f}"
        return str(x)
    cols = list(df.columns)
    widths = [max(len(c), *(len(fmt_val(v, c)) for v in df[c])) for c in cols] if not df.empty else [len(c) for c in cols]
    header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    sep    = "-+-".join("-"*w for w in widths)
    rows = [header, sep]
    for _, r in df.iterrows():
        rows.append(" | ".join(fmt_val(r[c], c).ljust(w) for c, w in zip(cols, widths)))
    return "\n".join(rows)

def main():
    ap = argparse.ArgumentParser(description="Run KPI SQL against DuckDB and output results.")
    ap.add_argument("--mode", choices=["compare","single"], default="compare", help="compare: last30 vs prior30; single: within [start,end]")
    ap.add_argument("--start", help="YYYY-MM-DD (only for mode=single)")
    ap.add_argument("--end",   help="YYYY-MM-DD (only for mode=single)")
    ap.add_argument("--group_by", help="Comma list of dimensions for single mode, e.g. platform,country")
    ap.add_argument("--db", default=DB_PATH, help=f"DuckDB path (default: {DB_PATH})")
    ap.add_argument("--format", choices=["table","json","csv"], default="table")
    ap.add_argument("--out", help="Optional file path to write output")
    args = ap.parse_args()

    if args.mode == "compare":
        payload = compute_kpi_compare(args.db)
        df = pd.DataFrame(payload["data"])
    else:
        payload = compute_kpi_single(args.start, args.end, args.group_by, args.db)
        df = pd.DataFrame(payload["data"])

    if args.format == "json":
        out = df.to_json(orient="records")
        if args.out: open(args.out, "w", encoding="utf-8").write(out)
        else: print(out)
    elif args.format == "csv":
        if args.out: df.to_csv(args.out, index=False)
        else: print(df.to_csv(index=False))
    else:
        print(to_pretty_table(df))

if __name__ == "__main__":
    main()

