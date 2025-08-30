# Marketing KPI Pipeline + AI Agent Demo (n8n + DuckDB + Flask API)

## Executive Summary  
This project delivers a **modular, production-style data pipeline** that ingests ad spend data, models core KPIs (CAC & ROAS), and exposes them through both a **Flask API** and a **Python CLI**, all orchestrated via **Docker Compose**.  

### Key Highlights  
- **Part 1 – Ingestion**: End-to-end ETL using n8n + SFTP + DuckDB, with provenance tracking and persistence.  
- **Part 2 – KPI Modeling**: Dynamic SQL queries calculate CAC and ROAS with safe math, comparing **Last 30 Days vs Prior 30 Days** or any custom date range.  
- **Part 3 – Analyst Access**: Metrics exposed via CLI, SQL, and a secure API, making them simple to consume for analysts and automation tools.  
- **Part 4 – Agent Demo**: Natural-language interface (`/ask`) proves how analyst questions can be mapped to KPI queries, and lays the foundation for future semantic retrieval with embeddings.  

This repo shows how to build a **realistic, containerized analytics workflow** from raw CSV ingestion to analyst-friendly KPI access—using modern, lightweight tools.  


# Setup Instructions

The entire project runs with **Docker Compose**, making setup simple and reproducible across Linux, macOS, and Windows.  
No complex installs of Python, DuckDB, or n8n are required.

---

## Prerequisites

- **Install Docker Desktop**  
  - [Docker Desktop for Linux](https://docs.docker.com/desktop/install/linux/)  
  - [Docker Desktop for macOS](https://docs.docker.com/desktop/install/mac/)  
  - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows/)  

- **Install Docker Compose**  
  Docker Desktop already includes it.  
  On Linux, if you installed only the Docker Engine, add Compose.

# Setup Instructions

## 1. Clone this repository

```git clone https://github.com/ccontreras17/n8n-duckdb-ai-pipeline.git```

```cd n8n-duckdb-ai-pipeline```

## 2. Environment Variables
Copy the template `.env` file and adjust as needed:
`cp example.env .env`

This file contains basic settings (like API key, timezone, also you can add your **OpenAI Key** for agent testing).

## 3. Start the Stack
**Linux**

`sudo docker compose pull`

`sudo docker compose up --build -d`

**macOS / Windows (with Docker Desktop)**

`docker compose pull`

`docker compose up --build -d`

- Containers spin up (`n8n`, `py-runner`, `sftp`).
- Volumes mount automatically (`landing/`, `warehouse/`, `sftp/upload/`).
- Your data warehouse lives in `warehouse/lake.duckdb`.
- n8n UI is available at http://localhost:5678.
- All required Python libraries are installed in the `py-runner` container at build time from `requirements.txt`
- The Flask API in py-runner auto-starts with the container and exposes endpoints (`/health`, `/load`, `/metrics`, `/ask`) immediately after docker compose up.


## 4. Verify Services
List running services:

`sudo docker compose ps`

Check logs to confirm that all Python dependencies finish installing and the Flask API has started (this may take a while on the first run):

`sudo docker compose logs -f py-runner`

## 5. Stop / Restart
Stop containers:

`sudo docker compose down`

Restart:

`sudo docker compose up --build -d`

## 6. Where Data Lives
- **Landing zone:** `./landing/` → where ingested CSVs are staged.
- **Warehouse:** `./warehouse/lake.duckdb` → persistent DuckDB file.
- **SFTP upload:** `./sftp/upload/` → simulates external data source.
- **n8n data:** `./n8n_data/` → all workflows & credentials are saved here.

## 7. CLI Help

**Help**

```sudo docker compose exec python python /workspace/scripts/run_sql_metrics.py -h```



---

# Part 1 – Ingestion

### Dataset
The base dataset used for this project is **`ads_spend.csv`.**

---

### Solution Architecture

To simulate a realistic ingestion workflow, the project runs inside a **Docker Compose stack** with three coordinated services:

- **n8n** – workflow automation tool that orchestrates ingestion jobs  
- **sftp** – local SFTP server simulating an external secure data source  
- **py-runner** – Python container for data cleaning, loading, and APIs  

Each service mounts volumes into the host project folder for persistence:
- `./sftp/upload/` → external source (CSV lives here)  
- `./landing/` → landing zone where n8n drops files  
- `./warehouse/` → DuckDB file (`lake.duckdb`) storing the canonical warehouse table  

---

### Ingestion Workflow

1. **Source**: CSVs are placed in `/sftp/upload/` (mounted from host).  
2. **Orchestration**: n8n connects to the SFTP service and pulls the file matching `ads_spend.csv`.  
3. **Landing**: n8n saves these into the `landing/` folder with a timestamped name.  
4. **Load**: n8n triggers the `py-runner` service at `http://py-runner:5000/load`.  
5. **Transform & Persist**: Python scripts:
   - Clean filenames (strip newlines, tabs, normalize casing)  
   - Normalize `spend` values (remove `$`, commas, handle negatives in parentheses)  
   - Enforce schema for all fields  
   - Add **provenance metadata** (`load_date`, `source_file_name`)  
   - Insert into DuckDB table `ads_spend`  

---

### Persistence Guarantee

- The DuckDB file (`lake.duckdb`) lives in the `warehouse/` volume.  
- New loads **append only new files** and skip already ingested ones.  
- This ensures that data **persists after refresh** and provenance is always visible.  

---

### Why Docker?

- **Reproducibility**: Each service runs in a controlled environment.  
- **Isolation**: n8n, SFTP, and Python runner don’t interfere with each other.  
- **Persistence**: Volumes ensure state (workflows, landing files, warehouse) survives container restarts.  
- **Realism**: Mirrors how ingestion pipelines are deployed in real-world teams.  

---

# Part 2 – KPI Modeling (SQL)

### Modeling
Compute two core acquisition KPIs from the ingested `ads_spend` table:

- **CAC** = `spend / conversions`
- **ROAS** = `revenue / spend`, with **`revenue = conversions * 100`**

The analysis compares **Last 30 Days** vs **Prior 30 Days**, anchored to the **latest date present in the data or today’s date, whichever is earlier** (so demos stay realistic even if the dataset is not updated). All outputs include **absolute values** and **% deltas**.

---

### How the solution is built

**Data warehouse:** DuckDB (`warehouse/lake.duckdb`)  
**Modeling layer:** SQL Script
**Access:** Query DuckDB via Pythton CLI or the Flask API

1. **KPI view**  
   We create a lightweight SQL script that aggregates by date and computes:
   - `spend`, `conversions`, derived `revenue = conversions * 100`
   - Guardrails to avoid division-by-zero:
     - `cac = spend / NULLIF(conversions, 0)`
     - `roas = revenue / NULLIF(spend, 0)`

2. **Comparison (L30 vs P30)**  
   The SQL query finds the **anchor date**, builds two 30-day windows, sums KPIs, and returns a **compact table.**


**API Usage**

```curl -s -X GET -H "X-API-Key: secret-key" "http://localhost:5000/metrics?mode=compare" | python -m json.tool```

**CLI Usage**

```sudo docker compose exec python python /workspace/scripts/run_sql_metrics.py --mode compare```

---

### Why this approach
- **Simple + transparent:** Pure SQL, easy to review and extend.
- **Resilient:** Safe math with `NULLIF` prevents noisy “Inf/NaN” KPIs.
- **Realistic time windows:** Auto-anchors to the newest available day or today whichever is earlier.
- **Composable:** The same logic can be grouped by `platform`, `country`, or any dimension by adding `GROUP BY` later.

---

# Part 3 – Analyst Access

### Goal
Once KPIs are modeled, they need to be accessible for quick analysis.  
Metrics were exposed in a **simple, reproducible way** so that analysts (or downstream workflows in n8n) can query CAC and ROAS directly.

---

### SQL Script + CLI + API

The metrics core is a modular SQL engine built on **DuckDB + Pandas** that powers both the API and CLI.  
It computes **Customer Acquisition Cost (CAC)** and **Return on Ad Spend (ROAS)** in two main modes:

- **Compare mode** → last 30 days vs prior 30 days.  
- **Single mode** → aggregate KPIs for any custom date range, with optional grouping by dimensions like `platform`, `campaign`, or `country`.

This modular design ensures consistency: the same SQL functions are used by the CLI, Flask API, and n8n workflows.

### Example Usage


### Single-window mode: aggregate CAC/ROAS in a custom date range
sudo docker compose exec python python /workspace/scripts/run_sql_metrics.py --mode single --start 2025-06-01 --end 2025-06-30

### Single-window + grouping 
sudo docker compose exec python python /workspace/scripts/run_sql_metrics.py --mode single --start 2025-06-01 --end 2025-06-30 --group_by platform

### Multiple groupings (comma-separated)
sudo docker compose exec python python /workspace/scripts/run_sql_metrics.py --mode single --start 2025-06-01 --end 2025-06-30 --group_by platform,country

### API Usage

curl -s -X GET "http://localhost:5000/metrics?start=2025-06-01&end=2025-06-30" -H "X-API-Key: secret-key" | python -m json.tool


### Part 4 – AI Agent Demo

### Goal
Show how a natural-language question can be mapped to the correct KPI query, proving that the pipeline could be extended into a lightweight AI agent.

---

### Example Question

**Compare CAC and ROAS for last 30 days vs prior 30 days.**

---

### Intent Detection
A simple rule-based check inside the Flask API looks for:
- `CAC` or `ROAS` → signals KPI intent  
- `compare` → signals we want a 2-window analysis  
- `last 30` + `prior 30` → signals time windows  

When all patterns match, the API triggers the **30-day comparison query** from Part 2.

---

### Endpoint
Demo:

POST /ask

Headers: X-API-Key: secret-key

Body: { "question": "Compare CAC and ROAS for last 30 days vs prior 30 days." }

---

### Example Call
---
```
curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: secret-key" \
  -d '{"question":"Compare CAC and ROAS for last 30 days vs prior 30 days."}' \
  http://localhost:5000/ask \
  | python -m json.tool
```

---

### AI Agent Summary

- **Bridges analysts and automation**  
  Queries can be triggered by plain English instead of rigid SQL syntax.  
  Analysts ask *“Compare CAC and ROAS for last 30 days vs prior 30 days”* and the pipeline maps it automatically.

- **Extensible with semantic retrieval**  
  Instead of hard-coded keyword matching, the system could embed analyst questions using a lightweight transformer model (e.g. **MiniLM**) and perform **vector similarity search** against a library of supported query templates.  
  This makes the system robust to variations like:  
  - *“How did our CAC trend vs ROAS over the past month compared to the month before?”*  
  - *“Show me the last 30 days vs the prior 30 for acquisition efficiency metrics.”*  

- **Lightweight AI agent**  
  Not a full NL→SQL solution, but a working proof of concept for **intelligent automation**:  
  1. Natural-language input → embedding with MiniLM.  
  2. Semantic search → retrieve the closest pre-defined KPI query.  
  3. Execute the mapped SQL.  
  4. Return JSON + optional friendly summary.  

- **Future-proof design**  
  By swapping the retrieval layer (regex → embeddings), this architecture scales from a demo into a real AI agent without major changes to the ingestion, modeling, or API layers.


