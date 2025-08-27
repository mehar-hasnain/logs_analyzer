import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.graph_objs as go
from jinja2 import Environment, FileSystemLoader
from plotly.offline import plot

from src.calo_logs_analyzer.anomalies import detect_anomalies
from src.calo_logs_analyzer.compute import build_ledger, build_reconciliation, summarize
from src.calo_logs_analyzer.parser import parse_logs

logger = logging.getLogger(__name__)


def _ensure_out(out_dir: str | Path) -> Path:
    """
    Ensure an output directory exists and is writable. If not writable, fall back to /tmp/out.
    """
    out_path = Path(out_dir).expanduser()
    try:
        out_path.mkdir(parents=True, exist_ok=True)
        # Test write
        test_file = out_path / ".write_test"
        with test_file.open("w") as f:
            f.write("ok")
        test_file.unlink()
        return out_path
    except Exception as exc:
        logger.warning(f"Unable to write to {out_path}: {exc}. Falling back to temporary directory.")
        fallback = Path(tempfile.gettempdir()) / "out"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _naive_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Make tz-aware datetime columns Excel-safe (UTC -> naive)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in out.select_dtypes(include=["datetimetz"]).columns:
        out[c] = out[c].dt.tz_convert("UTC").dt.tz_localize(None)
    return out


def _write_excel(
        out_dir: Path,
        ledger: pd.DataFrame,
        recon: pd.DataFrame,
        anomalies: pd.DataFrame,
        summary: Dict[str, Any],
        *,
        ledger_columns: Optional[List[str]] = None,
        run_ts
) -> str:
    """
    Write the report.xlsx file with multiple sheets. Optionally filter columns for the Ledger sheet.

    Parameters
    ----------
    out_dir : Path
        Output directory.
    ledger : DataFrame
        Full ledger.
    recon : DataFrame
        Reconciliation view.
    anomalies : DataFrame
        Anomaly details.
    summary : dict
        Summary dict from summarize().
    ledger_columns : list or None
        Columns to include for the Ledger sheet. If None, all columns are written.

    Returns
    -------
    str
        Path to the written Excel file.
    """
    out_path = out_dir / f"report_{run_ts}.xlsx" if run_ts else "report.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Filter ledger columns if specified
    ledger_sheet = ledger.copy()
    if ledger_columns is not None:
        cols = [c for c in ledger_columns if c in ledger_sheet.columns]
        ledger_sheet = ledger_sheet[cols].copy()

    # Overdrafts sheet: subset where overdraft is True
    overdrafts_sheet = ledger[ledger.get("overdraft", False) == True].copy()

    sheets = {
        "Ledger": ledger_sheet,
        "Reconciliation": recon,
        "ByUser": summary["by_user"],
        "BySource": summary["by_source"],
        "Overdrafts": overdrafts_sheet,
        "Anomalies": anomalies,
    }

    with pd.ExcelWriter(out_path, engine="openpyxl") as xl:
        for name, df_ in sheets.items():
            _naive_utc(df_).to_excel(xl, sheet_name=name, index=False)

    return str(out_path)


def _fig_total_by_type(ledger: pd.DataFrame):
    agg = ledger.groupby("type")["amount"].sum().reset_index()
    return go.Figure([go.Bar(x=agg["type"], y=agg["amount"])]).update_layout(title="Total Amount by Type")


def _fig_top_overdrafts(ledger: pd.DataFrame):
    od = ledger[ledger["overdraft"] == True]
    if od.empty:
        return go.Figure().update_layout(title="Top Overdraft Users (none)")
    agg = od.groupby("userId")["amount"].sum().reset_index().sort_values("amount", ascending=False).head(10)
    return go.Figure([go.Bar(x=agg["userId"], y=agg["amount"])]).update_layout(
        title="Top Overdraft Users (sum of amounts)")


def _fig_flow_over_time(ledger: pd.DataFrame):
    df = ledger.copy()
    if df.empty:
        return go.Figure().update_layout(title="Daily Net Flow (none)")
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date
    credits = df[df["type"] == "CREDIT"].groupby("date")["amount"].sum()
    debits = df[df["type"] == "DEBIT"].groupby("date")["amount"].sum()
    net = credits.reindex(sorted(set(df["date"])), fill_value=0) - debits.reindex(sorted(set(df["date"])), fill_value=0)
    return go.Figure([go.Scatter(x=list(net.index), y=list(net.values), mode="lines+markers")]).update_layout(
        title="Daily Net Flow (Credit - Debit)")


def _render_html(
        out_dir: Path,
        ledger: pd.DataFrame,
        recon: pd.DataFrame,
        anomalies: pd.DataFrame,
        summary: Dict[str, Any],
        run_ts
) -> str:
    """
    Render the HTML report using Jinja2 and Plotly charts.
    """
    # Locate templates relative to project root (two parents up)
    base_dir = Path(__file__).resolve().parents[2]
    templates_dir = base_dir / "templates"
    env = Environment(loader=FileSystemLoader(str(templates_dir)))
    tmpl = env.get_template("report.html.j2")

    # Charts to HTML
    fig1 = _fig_total_by_type(ledger)
    fig2 = _fig_top_overdrafts(ledger)
    fig3 = _fig_flow_over_time(ledger)

    fig1_div = plot(fig1, include_plotlyjs="cdn", output_type="div")
    fig2_div = plot(fig2, include_plotlyjs=False, output_type="div")
    fig3_div = plot(fig3, include_plotlyjs=False, output_type="div")

    # Prepare samples
    overdrafts_df = ledger[ledger.get("overdraft", False) == True]
    top_overdrafts = overdrafts_df[[
        "timestamp", "userId", "id", "amount", "oldBalance", "newBalance"
    ]].sort_values("amount", ascending=False).head(20).to_dict(orient="records")

    mismatches_df = ledger[ledger.get("balanceMismatch", False) == True]
    top_mismatches = mismatches_df[[
        "timestamp", "userId", "id", "oldBalance", "amount", "expectedNewBalance",
        "newBalance", "suggestedAdjustment"
    ]].head(20).to_dict(orient="records")

    anomaly_records = anomalies.sort_values("timestamp", ascending=False).head(50).to_dict(
        orient="records") if not anomalies.empty else []

    html = tmpl.render(
        totals=summary["totals"],
        fig1=fig1_div,
        fig2=fig2_div,
        fig3=fig3_div,
        top_overdrafts=top_overdrafts,
        top_mismatches=top_mismatches,
        anomalies=anomaly_records,
    )
    out_path = out_dir / f"report_{run_ts}.html" if run_ts else "report.html"
    with out_path.open("w", encoding="utf-8") as f:
        f.write(html)
    return str(out_path)


def _load_column_preset(preset: str | Path | None) -> Optional[List[str]]:
    """
    Load a list of column names either from a preset name or a JSON file.

    Parameters
    ----------
    preset : str or Path or None
        - 'accounting': return the default accounting columns
        - 'full': return None (no filtering)
        - path: load JSON file containing list of columns

    Returns
    -------
    list or None
        List of columns to include, or None if no filtering.
    """
    if preset is None:
        return None
    if isinstance(preset, str):
        if preset.lower() == "full":
            return None
        if preset.lower() == "accounting":
            return [
                "timestamp",
                "userId",
                "id",
                "type",
                "source",
                "action",
                "oldBalance",
                "amount",
                "newBalance",
                "expectedNewBalance",
                "balanceMismatch",
                "continuityBreak",
                "overdraft",
                "overdraftReason",
                "suggestedAdjustment",
            ]
    try:
        preset_path = Path(preset).expanduser()
        with preset_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [str(c) for c in data]
    except Exception as exc:
        logger.warning(f"Could not load Excel columns preset from {preset}: {exc}")
    return None


def run_analysis(
        log_dir: str | Path,
        out_dir: str | Path,
        *,
        export_raw: bool = False,
        log_level: str = "INFO",
        tolerance: float = 0.005,
        decimals: int = 2,
        excel_columns: Optional[str] = None
):
    """
    High-level orchestration for parsing logs, building the ledger, detecting anomalies,
    and exporting reports.
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=numeric_level, format="%(levelname)s %(name)s: %(message)s")
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logger.info(f"Starting analysis on {log_dir}, outputting to {out_dir}")

    out_path = _ensure_out(out_dir)
    events = parse_logs(log_dir)
    logger.info(f"Building ledger from {len(events)} events")
    ledger = build_ledger(events, decimals=decimals, tolerance=tolerance)
    logger.info(f"Ledger contains {len(ledger)} balance transactions")
    recon = build_reconciliation(ledger)
    anomalies = detect_anomalies(ledger, mad_threshold=6.0, dup_time_window=60.0)
    summary = summarize(ledger)

    if export_raw:
        raw_path = out_path / "raw_parsed.csv"
        ledger.to_csv(raw_path, index=False)
        logger.info(f"Wrote raw parsed CSV to {raw_path}")

    columns_list = _load_column_preset(excel_columns)
    xlsx_path = _write_excel(out_path, ledger, recon, anomalies, summary, ledger_columns=columns_list, run_ts=run_ts)
    html_path = _render_html(out_path, ledger, recon, anomalies, summary, run_ts=run_ts)

    logger.info(f"Exported report: {html_path}")
    logger.info(f"Exported Excel: {xlsx_path}")

    return {"html": html_path, "xlsx": xlsx_path}


if __name__ == "__main___":
    run_analysis()
