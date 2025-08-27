import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def build_ledger(
        events: List[Dict[str, Any]],
        *,
        decimals: int = 2,
        tolerance: float = 0.005,
        currency_decimals: Optional[Dict[str, int]] = None,
) -> pd.DataFrame:
    """
    Build a per-transaction ledger from parsed events, computing expected balances,
    overdraft flags, continuity checks and suggested adjustments.

    Parameters
    ----------
    events : list of dict
        Parsed events from the parser.
    decimals : int, default 2
        Default number of decimal places to round to when currency not recognised.
    tolerance : float, default 0.005
        Absolute difference tolerance when comparing two monetary values for equality.
    currency_decimals : dict or None
        Mapping of currency code (upper-case) to number of decimal places. If None,
        a sensible default of {"SAR": 3, "BHD": 4} will be used.

    Returns
    -------
    pd.DataFrame
        Ledger dataframe with computed fields.
    """
    currency_decimals = currency_decimals or {"SAR": 3, "BHD": 4}
    df = pd.DataFrame(events)
    if df.empty:
        return df

    # Keep only balance sync transactions for ledger
    tx = df[df["eventType"] == "BALANCE_SYNC"].copy()
    if tx.empty:
        return tx

    # Coerce numerics (float)
    numeric_cols = ["amount", "vat", "oldBalance", "newBalance", "paymentBalance"]
    for col in numeric_cols:
        if col in tx.columns:
            tx[col] = pd.to_numeric(tx[col], errors="coerce")

    # Determine decimals for each row based on currency
    def get_decimals(cur: str) -> int:
        cur = (cur or "").upper()
        return currency_decimals.get(cur, decimals)

    # Precompute quantize exponents for each currency
    quant_exps: Dict[int, Decimal] = {}

    def get_quant(dps: int) -> Decimal:
        if dps not in quant_exps:
            quant_exps[dps] = Decimal("1").scaleb(-dps)
        return quant_exps[dps]

    # row-wise computation using Decimal
    expected_list = []
    mismatch_list = []
    overdraft_list = []
    overdraft_reason = []
    new_filled_list = []
    old_decimal_list = []
    new_decimal_list = []
    sug_adj_list = []

    for idx, row in tx.iterrows():
        cur = row.get("currency") or ""
        dps = get_decimals(cur)
        quant = get_quant(dps)

        # Convert to Decimal safely
        def to_dec(x):
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return None
            try:
                return Decimal(str(x))
            except (InvalidOperation, ValueError):
                return None

        amount = to_dec(row.get("amount"))
        vat = to_dec(row.get("vat"))
        old_bal = to_dec(row.get("oldBalance"))
        new_bal = to_dec(row.get("newBalance"))
        amt = amount or Decimal(0)
        v = vat or Decimal(0)
        old_b = old_bal or Decimal(0)

        # Compute expected new balance
        if str(row.get("type")).upper() == "CREDIT":
            exp_new = old_b + (amt - v)
        elif str(row.get("type")).upper() == "DEBIT":
            exp_new = old_b - (amt - v)
        else:
            exp_new = None

        # Quantize expected
        if exp_new is not None:
            exp_new_q = exp_new.quantize(quant, rounding=ROUND_HALF_UP)
        else:
            exp_new_q = None
        expected_list.append(float(exp_new_q) if exp_new_q is not None else np.nan)

        # Quantize actual new balance
        if new_bal is not None:
            new_bal_q = new_bal.quantize(quant, rounding=ROUND_HALF_UP)
            new_decimal_list.append(new_bal_q)
        else:
            new_bal_q = None
            new_decimal_list.append(None)

        old_decimal_list.append(old_b.quantize(quant, rounding=ROUND_HALF_UP) if old_bal is not None else None)

        # Balance mismatch determination
        if exp_new_q is not None and new_bal_q is not None:
            diff = abs(exp_new_q - new_bal_q)
            mismatch = diff > Decimal(str(tolerance))
        else:
            mismatch = False
        mismatch_list.append(mismatch)

        # Overdraft reason and flag
        reason = None
        expected_neg = False
        actual_neg = False
        if exp_new_q is not None and exp_new_q < 0:
            expected_neg = True
            reason = "expected<0"
        if new_bal_q is not None and new_bal_q < 0:
            actual_neg = True
            reason = "actual<0"
        if expected_neg and actual_neg:
            reason = "expected balance < 0, actual balance < 0"
        overdraft_list.append(reason is not None)
        overdraft_reason.append(reason)

        # newBalanceFilled: use new_bal_q if present, else expected
        if new_bal_q is not None:
            nf = new_bal_q
        elif exp_new_q is not None:
            nf = exp_new_q
        else:
            nf = None
        new_filled_list.append(nf)

        # Suggested adjustment
        if mismatch and exp_new_q is not None and new_bal_q is not None:
            sug = (exp_new_q - new_bal_q).quantize(quant, rounding=ROUND_HALF_UP)
            sug_adj_list.append(float(sug))
        else:
            sug_adj_list.append(0.0)

    tx = tx.copy()
    tx["expectedNewBalance"] = expected_list
    tx["balanceMismatch"] = mismatch_list
    tx["overdraft"] = overdraft_list
    tx["overdraftReason"] = overdraft_reason
    tx["_newBalanceFilled"] = new_filled_list
    tx["_oldBalanceDec"] = old_decimal_list
    tx["_newBalanceDec"] = new_decimal_list
    tx["suggestedAdjustment"] = sug_adj_list

    # Determine continuity break per user with tolerance
    tx = tx.sort_values(["userId", "timestamp", "id", "messageId"])
    tx["prevNewBalanceFilled"] = tx.groupby("userId")["_newBalanceFilled"].shift(1)

    def cont_break(row) -> bool:
        ob_dec = row["_oldBalanceDec"]
        prev_nf = to_dec(row["prevNewBalanceFilled"])
        if ob_dec is None or prev_nf is None:
            return False
        diff = abs(ob_dec - prev_nf)
        return diff > Decimal(str(tolerance))

    tx["continuityBreak"] = tx.apply(cont_break, axis=1)

    # Cleanup temporary columns
    tx.drop(columns=["_newBalanceFilled", "_oldBalanceDec", "_newBalanceDec"], inplace=True)
    return tx


def build_reconciliation(ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Create a reconciliation table summarising key fields for accounting.
    """
    if ledger.empty:
        return ledger
    cols = [
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
    cols = [c for c in cols if c in ledger.columns]
    recon = ledger[cols].copy()
    return recon


def summarize(ledger: pd.DataFrame) -> Dict[str, Any]:
    if ledger.empty:
        return {
            "totals": {},
            "by_user": pd.DataFrame(),
            "by_source": pd.DataFrame(),
            "overdrafts": pd.DataFrame(),
        }
    totals = {
        "transactions": int(len(ledger)),
        "unique_users": int(ledger["userId"].nunique()),
        "total_debit": float(ledger.loc[ledger["type"] == "DEBIT", "amount"].sum()),
        "total_credit": float(ledger.loc[ledger["type"] == "CREDIT", "amount"].sum())
    }
    by_user = ledger.groupby("userId").agg(
        tx_count=("id", "count"),
        total_debit=("amount", lambda s: float(s[ledger.loc[s.index, "type"] == "DEBIT"].sum())),
        total_credit=("amount", lambda s: float(s[ledger.loc[s.index, "type"] == "CREDIT"].sum())),
        overdrafts=("overdraft", "sum"),
        mismatches=("balanceMismatch", "sum"),
        continuity_breaks=("continuityBreak", "sum"),
    ).reset_index()
    by_source = ledger.groupby(["source", "type"]).agg(total_amount=("amount", "sum"),
                                                       tx_count=("id", "count")).reset_index()
    overdrafts = ledger[ledger["overdraft"] == True].copy()
    return {"totals": totals, "by_user": by_user, "by_source": by_source, "overdrafts": overdrafts}
