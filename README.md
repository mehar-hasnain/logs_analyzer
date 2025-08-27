# Calo Balance Log Analytics

This project provides a **Docker‑packaged analytics tool** to parse Calo subscription balance logs, validate per‑user balances and continuity, detect overdrafts and accounting anomalies, and export human‑friendly reports.  The emphasis is on **accuracy, traceability and explainability** so that accountants can rely on the numbers without manual cross‑checks.

The tool accepts mixed plain text and gzipped logs (even when the `.gz` extension is missing), produces both an interactive **HTML dashboard** and an **Excel workbook**, and exposes a CLI with sensible defaults and advanced options.

## Features

-  **Robust log parser** — scans directories recursively, handles gzip or plain text, and extracts the `Start syncing the balance { transaction: {...} }` blocks as structured events.
-  **Ledger engine** — recomputes the expected new balance using high‑precision `Decimal`, applies currency‑specific rounding, tolerances and continuity rules, and flags overdrafts with clear reasons.
-  **Advanced anomaly detection** — invalid actions, MAD‑based spikes, rapid repeated deductions, duplicate IDs, bursts(multiple transactions for same user under 1 sec) , after‑hours/weekend postings, rounding patterns, currency mismatches.
-  **Curated reports** — generates:
  - `report.html` with cards and charts (total transactions, overdrafts, mismatches) and sample tables.
  - `report.xlsx` with sheets: **Ledger** (configurable columns), **Reconciliation**, **ByUser**, **BySource**, **Overdrafts** and **Anomalies**.
  - Optional `raw_parsed.csv` for full auditability.
-  **Cross‑platform Docker image** — zero system dependencies beyond Docker. Works on Windows, Mac and Linux.
-  **Configurable CLI** — set log level, tolerances, rounding decimals, and choose which columns appear in the Excel ledger sheet.

## Quick Start

1. **Prepare your data.**
   ```bash
   mkdir -p data out
   cp /path/to/your/logs/* data/
   ```

2. **Build the image.**
   ```bash
   docker build -t calo-log-analytics .
   ```

3. **Run the analyzer.**
   ```bash
   # Unix/macOS
   docker run --rm -v "$(pwd)/data:/data" -v "$(pwd)/out:/out" calo-log-analytics \
     --log_dir /data --out /out --excel-columns accounting --log-level INFO

   # Windows (PowerShell)
   docker run --rm -v ${PWD}\data:/data -v ${PWD}\out:/out calo-log-analytics `
     --log_dir /data --out /out --excel-columns accounting
   ```

4. **Review the outputs.**
   - `out/report.html` — open in your browser for an at‑a‑glance summary.
   - `out/report.xlsx` — open in Excel or Sheets for detailed tables.
   - `out/raw_parsed.csv` — optional raw dump if `--raw-csv` was specified.

## CLI Options

The CLI is invoked by running the container (or directly via `python -m calo_logs_analyzer`).  Key options:

```
usage: calo-logs-analyzer [-h] [--out OUT] [--raw-csv]
                         [--log-level {DEBUG,INFO,WARNING,ERROR}]
                         [--tolerance TOLERANCE] [--decimals DECIMALS]
                         [--excel-columns EXCEL_COLUMNS] 
                         log_dir

positional arguments:
  log_dir               Folder containing log files (can be plain or gzipped)

optional arguments:
  --out OUT             Output folder (default: ./out)
  --raw-csv             Also export raw parsed CSV
  --log-level           Logging level (default: INFO)
  --tolerance           Absolute tolerance when comparing balances (default: 0.005)
  --decimals            Default decimal places when currency is unknown (default: 2)
  --excel-columns       Columns preset for the Excel ledger sheet:
                        'accounting', 'full' or path to a JSON file
```

Use `--excel-columns accounting` to produce a slimmed ledger with only the columns needed by accountants.  Use `--excel-columns full` to include all fields, or provide a custom JSON file listing the columns.

## Outputs

- **HTML Report** (`report.html`) — summarises total transactions, unique users, total debits/credits, and visualises daily net flow, top overdrafts, mismatches and anomalies.
- **Excel Workbook** (`report.xlsx`) — contains:
  - **Ledger** — configurable view of all transactions with computed fields.
  - **Reconciliation** — key columns for spotting mismatches and continuity breaks.
  - **ByUser** and **BySource** — aggregations for management reporting.
  - **Overdrafts** — subset of ledger where the expected or actual new balance drops below zero, with a clear reason and suggested adjustment.
  - **Anomalies** — one row per detected anomaly (type and details).
- **Raw CSV** (`raw_parsed.csv`) — optional full dump of parsed events for auditing.

## Module Overview

The codebase is organised under `src/calo_logs_analyzer`:

- **`parser.py`** — reads log files (plain or gzip), matches the transaction blocks and info lines via regex, coerces values, rounds monetary fields per currency and outputs a list of event dicts.  Uses `Path` and logs progress.

- **`compute.py`** — constructs a ledger from events:
  - Recomputes expected new balances using `Decimal` with currency‑specific rounding.
  - Flags balance mismatches within a configurable tolerance.
  - Detects overdrafts (`expected<0`, `actual<0` or `both`) and continuity breaks.
  - Adds a suggested adjustment when there is a mismatch.
  - Returns a DataFrame ready for summarisation and export.

- **`anomalies.py`** — inspects the ledger to surface unusual patterns:
  - Invalid actions (typos like `INVAILID_ENTRY`).
  - Median Absolute Deviation (MAD) spikes per user/type.
  - Duplicate transaction IDs and rapid repeated manual deductions.
  - Bursts transactions.
  Each anomaly is emitted as its own row with a type and details.

- **`report.py`** — orchestrates the analysis:
  - Sets up logging, parses the logs, builds the ledger and reconciliation tables.
  - Calls anomaly detection and summarisation.
  - Writes the Excel workbook, filtering the Ledger sheet via a column preset.
  - Renders the HTML dashboard with Jinja2 and Plotly.
  - Handles output directories portably and falls back to `/tmp/out` if necessary.

- **`__main__.py`** — defines the CLI entrypoint; parses arguments and delegates to `run_analysis`.

## Accuracy Considerations

Accounting requires determinate, auditable numbers.  This tool:

- Uses Python’s `decimal.Decimal` for all monetary computations.
- Applies per‑currency decimal places (defaults: SAR->3, BHD->4, else 2) and rounds half up.
- Allows a tolerance for comparing computed and logged balances (default ±0.005) to avoid false positives from rounding noise.
- Orders transactions deterministically by `(userId, timestamp, id, messageId)` before checking continuity.
- Fills missing `newBalance` with the recomputed expected value so that continuity uses the best available figure.

## Scaling and Future Work

While pandas suffices for typical Lambda log volumes, larger deployments may require distributed processing.  Two paths forward:

1. **PySpark** — You can port the parser and ledger logic into a Spark job, storing events in a DataFrame and performing group-based analyses across a cluster.  PySpark’s `pyspark.sql.functions` support many of the same operations (window functions, MAD approximations, etc.).
2. **Snowflake/Snowpark** — Stage your logs into an external stage in Snowflake, create an external table over them, and use Snowflake’s semi‑structured functions (`VARIANT`, `FLATTEN`) or Snowpark (Python in‑warehouse) to parse the JSON blocks.  You can then materialise ledgers and anomaly tables for BI or alerting.  The tolerance and rounding logic can be expressed via Snowflake’s numeric types.

Other improvements on the horizon:

- Persist starting balances from a source‑of‑truth ledger and cross‑validate with the computed ledger.
- Hook into an alerting system (email, Slack) when overdrafts or high‑severity anomalies occur.
- Add PII‑safe hashing and role‑based access to restrict who sees which fields.
- Formalise the schema and add CI tests and fuzzing for the parser.
