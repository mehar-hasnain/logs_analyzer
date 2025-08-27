import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def first_digit(val: float) -> Optional[int]:
    """
    Extract the first non-zero digit of the absolute value.
    """
    if val is None or np.isnan(val):
        return None
    s = str(abs(val))
    for ch in s:
        if ch.isdigit() and ch != "0":
            return int(ch)
    return None


def detect_anomalies(
        ledger: pd.DataFrame,
        *,
        mad_threshold: float = 6.0,
        dup_time_window: float = 60.0
) -> pd.DataFrame:
    """
    Identify potential anomalies in the ledger. Each anomaly is returned as its
    own row with a type and details message.

    Parameters
    ----------
    ledger : pd.DataFrame
        Ledger produced by build_ledger.
    mad_threshold : float, default 6.0
        Threshold for MAD-based spike detection. Larger values reduce sensitivity.
    dup_time_window : float, default 60.0
        Window in seconds to consider rapid repeated identical amounts as duplicates.

    Returns
    -------
    pd.DataFrame
        DataFrame with anomaly rows (timestamp, userId, id, type, source, action, amount,
        oldBalance, newBalance, anomalyType, details).
    """
    if ledger is None or ledger.empty:
        return pd.DataFrame(columns=[
            "timestamp", "userId", "id", "type", "source", "action", "amount", "oldBalance",
            "newBalance", "anomalyType", "details"
        ])
    df = ledger.copy()
    # Ensure timestamp is datetime64[ns, UTC]
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    # Sort for sequential checks
    df = df.sort_values(["userId", "timestamp", "id"])

    anomalies: List[Dict[str, Any]] = []

    # Invalid actions (typos)
    invalid_mask = df["action"].astype(str).str.contains("INVALID|INVAILID", case=False, na=False)
    for _, row in df[invalid_mask].iterrows():
        anomalies.append({
            "timestamp": row["timestamp"],
            "userId": row["userId"],
            "id": row["id"],
            "type": row["type"],
            "source": row["source"],
            "action": row["action"],
            "amount": row["amount"],
            "oldBalance": row["oldBalance"],
            "newBalance": row["newBalance"],
            "anomalyType": "InvalidAction",
            "details": "Action contains 'INVALID'",
        })

    # MAD-based spikes per user/type on amount
    grouped = df.groupby(["userId", "type"])
    for (uid, typ), grp in grouped:
        amounts = grp["amount"].astype(float)
        if len(amounts) == 0:
            continue
        median = np.nanmedian(amounts)
        mad = np.nanmedian(np.abs(amounts - median))
        if mad == 0 or np.isnan(mad):
            continue
        # MAD z-score
        mad_z = np.abs((amounts - median) / mad)
        spike_mask = mad_z >= mad_threshold
        for _, row in grp[spike_mask].iterrows():
            anomalies.append({
                "timestamp": row["timestamp"],
                "userId": row["userId"],
                "id": row["id"],
                "type": row["type"],
                "source": row["source"],
                "action": row["action"],
                "amount": row["amount"],
                "oldBalance": row["oldBalance"],
                "newBalance": row["newBalance"],
                "anomalyType": "MADSpike",
                "details": f"MAD z-score {float(np.abs((row['amount'] - median) / mad)):.2f} >= {mad_threshold}",
            })

    # Duplicate transaction ids per user
    dup_id_mask = df.duplicated(subset=["userId", "id"], keep=False)
    for _, row in df[dup_id_mask].iterrows():
        anomalies.append({
            "timestamp": row["timestamp"],
            "userId": row["userId"],
            "id": row["id"],
            "type": row["type"],
            "source": row["source"],
            "action": row["action"],
            "amount": row["amount"],
            "oldBalance": row["oldBalance"],
            "newBalance": row["newBalance"],
            "anomalyType": "DuplicateTxId",
            "details": "Duplicate transaction id for user",
        })

    for col in ["type", "source", "action"]:
        # blank = NaN OR all-whitespace
        blank_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        if blank_mask.any():
            for _, row in df[blank_mask].iterrows():
                anomalies.append({
                    "timestamp": row["timestamp"],
                    "userId": row["userId"],
                    "id": row["id"],
                    "type": row.get("type"),
                    "source": row.get("source"),
                    "action": row.get("action"),
                    "amount": row.get("amount"),
                    "oldBalance": row.get("oldBalance"),
                    "newBalance": row.get("newBalance"),
                    "anomalyType": "MissingField",
                    "details": f"{col} is blank",
                })

    # Rapid repeated manual deductions within dup_time_window
    df["prevTimestamp"] = df.groupby(["userId", "type", "amount"])["timestamp"].shift(1)
    rapid_mask = (
            (df["type"] == "DEBIT") &
            (df["source"].astype(str).str.contains("MANUAL", case=False, na=False)) &
            (df["prevTimestamp"].notna()) &
            ((df["timestamp"] - df["prevTimestamp"]).dt.total_seconds() <= dup_time_window)
    )
    for _, row in df[rapid_mask].iterrows():
        anomalies.append({
            "timestamp": row["timestamp"],
            "userId": row["userId"],
            "id": row["id"],
            "type": row["type"],
            "source": row["source"],
            "action": row["action"],
            "amount": row["amount"],
            "oldBalance": row["oldBalance"],
            "newBalance": row["newBalance"],
            "anomalyType": "RapidManualDeduction",
            "details": f"Repeated manual {row['type']} within {dup_time_window}s",
        })

    # Continuity breaks flagged in ledger
    if "continuityBreak" in df.columns:
        cont_mask = df["continuityBreak"] == True
        for _, row in df[cont_mask].iterrows():
            anomalies.append({
                "timestamp": row["timestamp"],
                "userId": row["userId"],
                "id": row["id"],
                "type": row["type"],
                "source": row["source"],
                "action": row["action"],
                "amount": row["amount"],
                "oldBalance": row["oldBalance"],
                "newBalance": row["newBalance"],
                "anomalyType": "ContinuityBreak",
                "details": "Old balance does not match previous new balance",
            })

    # Balance mismatches flagged in ledger
    if "balanceMismatch" in df.columns:
        mismatch_mask = df["balanceMismatch"] == True
        for _, row in df[mismatch_mask].iterrows():
            anomalies.append({
                "timestamp": row["timestamp"],
                "userId": row["userId"],
                "id": row["id"],
                "type": row["type"],
                "source": row["source"],
                "action": row["action"],
                "amount": row["amount"],
                "oldBalance": row["oldBalance"],
                "newBalance": row["newBalance"],
                "anomalyType": "BalanceMismatch",
                "details": f"Expected {row['expectedNewBalance']} != Actual {row['newBalance']}",
            })

    # Large gaps or bursts
    df["prevTs"] = df.groupby("userId")["timestamp"].shift(1)
    df["gapSeconds"] = (df["timestamp"] - df["prevTs"]).dt.total_seconds()
    gap_mask = df["gapSeconds"].notna() & (df["gapSeconds"] < 1)
    for _, row in df[gap_mask].iterrows():
        anomalies.append({
            "timestamp": row["timestamp"],
            "userId": row["userId"],
            "id": row["id"],
            "type": row["type"],
            "source": row["source"],
            "action": row["action"],
            "amount": row["amount"],
            "oldBalance": row["oldBalance"],
            "newBalance": row["newBalance"],
            "anomalyType": "Burst",
            "details": "Transactions within <1s of each other",
        })

    # Currency consistency per user
    currency_counts = df.groupby("userId")["currency"].nunique()
    mixed_users = currency_counts[currency_counts > 1].index.tolist()
    for _, row in df[df["userId"].isin(mixed_users)].iterrows():
        anomalies.append({
            "timestamp": row["timestamp"],
            "userId": row["userId"],
            "id": row["id"],
            "type": row["type"],
            "source": row["source"],
            "action": row["action"],
            "amount": row["amount"],
            "oldBalance": row["oldBalance"],
            "newBalance": row["newBalance"],
            "anomalyType": "CurrencyMismatch",
            "details": "Multiple currencies detected for same user",
        })

    if not anomalies:
        return pd.DataFrame(columns=[
            "timestamp", "userId", "id", "type", "source", "action", "amount", "oldBalance",
            "newBalance", "anomalyType", "details"
        ])
    anomalies_df = pd.DataFrame(anomalies)
    anomalies_df = anomalies_df.sort_values("timestamp")
    return anomalies_df
