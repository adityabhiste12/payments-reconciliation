"""
Payment Platform & Bank Settlement Reconciliation System
========================================================
Generates synthetic data and performs full reconciliation analysis.
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

# ── Reproducibility ──────────────────────────────────────────────────────────
np.random.seed(42)

# ════════════════════════════════════════════════════════════════════════════
# 1.  SYNTHETIC DATA GENERATION
# ════════════════════════════════════════════════════════════════════════════

def generate_transactions() -> pd.DataFrame:
    """Platform transaction ledger (source of truth)."""

    base_date = date(2024, 3, 1)

    records = [
        # ── Normal transactions ───────────────────────────────────────────
        {"txn_id": "TXN-001", "date": base_date + timedelta(days=0),  "merchant": "Alpha Corp",   "amount":  1500.00, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-002", "date": base_date + timedelta(days=1),  "merchant": "Beta Ltd",     "amount":   320.75, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-003", "date": base_date + timedelta(days=3),  "merchant": "Gamma Inc",    "amount":   875.50, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-004", "date": base_date + timedelta(days=5),  "merchant": "Delta Co",     "amount":  2200.00, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-005", "date": base_date + timedelta(days=7),  "merchant": "Epsilon LLC",  "amount":   450.33, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-006", "date": base_date + timedelta(days=10), "merchant": "Zeta GmbH",    "amount":  3100.00, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-007", "date": base_date + timedelta(days=12), "merchant": "Eta SA",       "amount":   780.20, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-008", "date": base_date + timedelta(days=14), "merchant": "Theta BV",     "amount":  1250.00, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-009", "date": base_date + timedelta(days=16), "merchant": "Iota AG",      "amount":   560.80, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-010", "date": base_date + timedelta(days=18), "merchant": "Kappa Pty",    "amount":   990.45, "currency": "USD", "type": "payment"},
        # ── ANOMALY 1 : settled in the following month (TXN-011) ──────────
        {"txn_id": "TXN-011", "date": base_date + timedelta(days=28), "merchant": "Lambda NV",    "amount":  1800.00, "currency": "USD", "type": "payment"},
        # ── ANOMALY 2 : rounding difference on TXN-012 ───────────────────
        #    Platform records .335 — when summed at two-decimal precision
        #    the bank rounds to .34 (+0.005 per unit; visible only in bulk)
        {"txn_id": "TXN-012", "date": base_date + timedelta(days=20), "merchant": "Mu Corp",      "amount":   445.335,"currency": "USD", "type": "payment"},
        # ── ANOMALY 3 : TXN-013 will be duplicated in settlements ─────────
        {"txn_id": "TXN-013", "date": base_date + timedelta(days=22), "merchant": "Nu Pte",       "amount":   670.00, "currency": "USD", "type": "payment"},
        # ── ANOMALY 4 : refund for a non-existent original ────────────────
        {"txn_id": "TXN-REF-999", "date": base_date + timedelta(days=25), "merchant": "Xi Trading",
         "amount": -250.00, "currency": "USD", "type": "refund", "original_txn_id": "TXN-GHOST-000"},
        # ── Extra normal transactions ─────────────────────────────────────
        {"txn_id": "TXN-014", "date": base_date + timedelta(days=8),  "merchant": "Omicron SA",   "amount":   310.60, "currency": "USD", "type": "payment"},
        {"txn_id": "TXN-015", "date": base_date + timedelta(days=15), "merchant": "Pi Holdings",  "amount":  4500.00, "currency": "USD", "type": "payment"},
    ]

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df["amount"] = df["amount"].astype(float)
    df["original_txn_id"] = df.get("original_txn_id", np.nan)
    df = df[["txn_id", "date", "merchant", "amount", "currency", "type", "original_txn_id"]]
    return df


def generate_settlements(transactions: pd.DataFrame) -> pd.DataFrame:
    """Bank settlement file — mirrors transactions with deliberate anomalies."""

    # Start from normal (non-refund) payments
    normal = transactions[transactions["type"] == "payment"].copy()

    settlements = []

    for _, row in normal.iterrows():
        tid = row["txn_id"]

        # ANOMALY 1 – late settlement: TXN-011 settles on April 2 (next month)
        if tid == "TXN-011":
            settle_date = pd.Timestamp("2024-04-02")
        else:
            settle_date = row["date"] + pd.Timedelta(days=np.random.randint(1, 3))

        # ANOMALY 2 – bank rounds TXN-012 to 2 decimal places (.335 → .34)
        if tid == "TXN-012":
            amount = round(row["amount"], 2)   # 445.34  (platform has 445.335)
        else:
            amount = row["amount"]

        settlements.append({
            "settlement_id": f"SET-{tid}",
            "txn_ref":       tid,
            "settlement_date": settle_date,
            "merchant":      row["merchant"],
            "amount":        amount,
            "currency":      row["currency"],
            "status":        "settled",
        })

    # ANOMALY 3 – duplicate settlement for TXN-013
    dup = next(s for s in settlements if s["txn_ref"] == "TXN-013")
    dup_copy = dup.copy()
    dup_copy["settlement_id"] = "SET-TXN-013-DUP"
    dup_copy["settlement_date"] = dup["settlement_date"] + pd.Timedelta(days=1)
    settlements.append(dup_copy)

    # NOTE: TXN-REF-999 (orphan refund) intentionally absent from settlements.
    # NOTE: TXN-GHOST-000 does not exist in platform — orphan reference.

    df = pd.DataFrame(settlements)
    df["settlement_date"] = pd.to_datetime(df["settlement_date"])
    df["amount"] = df["amount"].astype(float)
    return df


# ════════════════════════════════════════════════════════════════════════════
# 2.  RECONCILIATION ENGINE
# ════════════════════════════════════════════════════════════════════════════

AMOUNT_TOLERANCE = 0.001   # amounts within this are considered matching


def reconcile(transactions: pd.DataFrame, settlements: pd.DataFrame) -> dict:
    """
    Full reconciliation pass.  Returns a dict with one DataFrame per issue type.
    """
    issues = {}

    # ── 2a. Duplicate settlements ────────────────────────────────────────────
    dup_mask = settlements.duplicated(subset=["txn_ref"], keep=False)
    issues["duplicate_settlements"] = (
        settlements[dup_mask]
        .sort_values("txn_ref")
        .reset_index(drop=True)
    )

    # Keep only the first occurrence per txn_ref for the remaining checks
    settlements_dedup = settlements.drop_duplicates(subset=["txn_ref"], keep="first")

    # ── 2b. Merge platform ↔ bank ────────────────────────────────────────────
    payments = transactions[transactions["type"] == "payment"].copy()
    merged = payments.merge(
        settlements_dedup[["settlement_id", "txn_ref", "settlement_date", "amount"]],
        left_on="txn_id", right_on="txn_ref",
        how="left",
        suffixes=("_platform", "_bank"),
    )

    # ── 2c. Missing settlements ──────────────────────────────────────────────
    issues["missing_settlements"] = (
        merged[merged["settlement_id"].isna()]
        [["txn_id", "date", "merchant", "amount_platform"]]
        .reset_index(drop=True)
    )

    # Work only with matched rows for the next checks
    matched = merged[merged["settlement_id"].notna()].copy()

    # ── 2d. Late settlements (different calendar month) ──────────────────────
    matched["txn_month"]    = matched["date"].dt.to_period("M")
    matched["settle_month"] = matched["settlement_date"].dt.to_period("M")
    late_mask = matched["txn_month"] != matched["settle_month"]
    issues["late_settlements"] = (
        matched[late_mask]
        [["txn_id", "merchant", "date", "settlement_date", "txn_month", "settle_month"]]
        .reset_index(drop=True)
    )

    # ── 2e. Amount mismatches ────────────────────────────────────────────────
    matched["amount_diff"] = (matched["amount_bank"] - matched["amount_platform"]).round(6)
    mismatch_mask = matched["amount_diff"].abs() > AMOUNT_TOLERANCE
    issues["amount_mismatches"] = (
        matched[mismatch_mask]
        [["txn_id", "merchant", "amount_platform", "amount_bank", "amount_diff"]]
        .reset_index(drop=True)
    )

    # ── 2f. Orphan refunds ───────────────────────────────────────────────────
    refunds = transactions[transactions["type"] == "refund"].copy()
    all_txn_ids = set(transactions["txn_id"])

    orphan_refunds = []
    for _, r in refunds.iterrows():
        orig = r.get("original_txn_id")
        if pd.isna(orig) or orig not in all_txn_ids:
            orphan_refunds.append({
                "refund_txn_id":    r["txn_id"],
                "date":             r["date"],
                "amount":           r["amount"],
                "merchant":         r["merchant"],
                "original_txn_id":  orig if not pd.isna(orig) else "N/A",
                "reason":           "original transaction not found",
            })
    issues["orphan_refunds"] = pd.DataFrame(orphan_refunds).reset_index(drop=True)

    return issues


# ════════════════════════════════════════════════════════════════════════════
# 3.  REPORT PRINTER
# ════════════════════════════════════════════════════════════════════════════

SEPARATOR = "═" * 70

def section(title: str):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)

def print_report(transactions: pd.DataFrame, settlements: pd.DataFrame, issues: dict):
    print("\n" + SEPARATOR)
    print("  PAYMENT PLATFORM ↔ BANK SETTLEMENT — RECONCILIATION REPORT")
    print(f"  Report Date : {date.today()}")
    print(SEPARATOR)

    # ── Summary ──────────────────────────────────────────────────────────────
    n_payments  = len(transactions[transactions["type"] == "payment"])
    n_refunds   = len(transactions[transactions["type"] == "refund"])
    n_settles   = len(settlements)
    n_dups      = len(issues["duplicate_settlements"]) // 2        # pairs
    n_missing   = len(issues["missing_settlements"])
    n_late      = len(issues["late_settlements"])
    n_mismatch  = len(issues["amount_mismatches"])
    n_orphan    = len(issues["orphan_refunds"])
    total_issues = n_missing + n_late + n_mismatch + n_dups + n_orphan

    section("SUMMARY")
    print(f"  Platform transactions (payments) : {n_payments:>5}")
    print(f"  Platform transactions (refunds)  : {n_refunds:>5}")
    print(f"  Bank settlement records          : {n_settles:>5}")
    print(f"  {'─'*42}")
    print(f"  ⚠  Total discrepancies found     : {total_issues:>5}")
    print(f"     • Missing settlements          : {n_missing:>5}")
    print(f"     • Late settlements             : {n_late:>5}")
    print(f"     • Amount mismatches            : {n_mismatch:>5}")
    print(f"     • Duplicate settlement groups  : {n_dups:>5}")
    print(f"     • Orphan refunds               : {n_orphan:>5}")

    # ── 1. Missing settlements ────────────────────────────────────────────────
    section("1 · MISSING SETTLEMENTS")
    df = issues["missing_settlements"]
    if df.empty:
        print("  ✅  None found.")
    else:
        print(f"  {len(df)} payment(s) have NO matching settlement record.\n")
        print(df.to_string(index=False))

    # ── 2. Late settlements ───────────────────────────────────────────────────
    section("2 · LATE SETTLEMENTS  (cross-month)")
    df = issues["late_settlements"]
    if df.empty:
        print("  ✅  None found.")
    else:
        print(f"  {len(df)} payment(s) were settled in a different calendar month.\n")
        for _, row in df.iterrows():
            print(f"  • {row['txn_id']:12s}  {row['merchant']}")
            print(f"    Transaction date : {row['date'].date()}"
                  f"  (month: {row['txn_month']})")
            print(f"    Settlement date  : {row['settlement_date'].date()}"
                  f"  (month: {row['settle_month']})")

    # ── 3. Amount mismatches ──────────────────────────────────────────────────
    section("3 · AMOUNT MISMATCHES")
    df = issues["amount_mismatches"]
    if df.empty:
        print("  ✅  None found.")
    else:
        print(f"  {len(df)} record(s) with amount discrepancies "
              f"(tolerance: ±{AMOUNT_TOLERANCE}).\n")
        for _, row in df.iterrows():
            direction = "OVER-SETTLED" if row["amount_diff"] > 0 else "UNDER-SETTLED"
            print(f"  • {row['txn_id']:12s}  {row['merchant']}")
            print(f"    Platform amount  : {row['amount_platform']:>10.4f}")
            print(f"    Bank amount      : {row['amount_bank']:>10.4f}")
            print(f"    Difference       : {row['amount_diff']:>+10.4f}  ← {direction}")
            print(f"    Note: difference arises from bank rounding "
                  f"(.335 → .34); invisible per-row but surfaces in bulk sums.")
            print()

    # ── 4. Duplicate settlements ──────────────────────────────────────────────
    section("4 · DUPLICATE SETTLEMENTS")
    df = issues["duplicate_settlements"]
    if df.empty:
        print("  ✅  None found.")
    else:
        grouped = df.groupby("txn_ref")
        print(f"  {len(grouped)} transaction(s) have >1 settlement record.\n")
        for txn_ref, grp in grouped:
            over_settled = grp["amount"].sum() - grp["amount"].iloc[0]
            print(f"  • Transaction ref  : {txn_ref}")
            print(f"    Duplicate count  : {len(grp)}")
            print(f"    Over-settled by  : {over_settled:>10.2f}")
            print(f"    Settlement IDs   : {', '.join(grp['settlement_id'].tolist())}")
            print()

    # ── 5. Orphan refunds ─────────────────────────────────────────────────────
    section("5 · ORPHAN REFUNDS  (no matching original)")
    df = issues["orphan_refunds"]
    if df.empty:
        print("  ✅  None found.")
    else:
        print(f"  {len(df)} refund(s) reference a non-existent original transaction.\n")
        for _, row in df.iterrows():
            print(f"  • Refund ID        : {row['refund_txn_id']}")
            print(f"    Date             : {row['date'].date()}")
            print(f"    Merchant         : {row['merchant']}")
            print(f"    Amount           : {row['amount']:>10.2f}")
            print(f"    Referenced orig  : {row['original_txn_id']}")
            print(f"    Issue            : {row['reason']}")
            print()

    # ── Footer ────────────────────────────────────────────────────────────────
    section("END OF REPORT")
    status = "⚠  ACTION REQUIRED" if total_issues else "✅  FULLY RECONCILED"
    print(f"  Status: {status}")
    print()


# ════════════════════════════════════════════════════════════════════════════
# 4.  MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    print("Generating synthetic datasets …")
    transactions = generate_transactions()
    settlements  = generate_settlements(transactions)

    print(f"  • Transactions : {len(transactions)} rows")
    print(f"  • Settlements  : {len(settlements)} rows")

    print("\nRunning reconciliation …")
    issues = reconcile(transactions, settlements)

    print_report(transactions, settlements, issues)

    # ── Optional: export raw datasets for inspection ──────────────────────────
    transactions.to_csv("data/transactions.csv", index=False)
    settlements.to_csv("data/settlements.csv", index=False)
    print("Raw CSVs written to outputs/transactions.csv & outputs/settlements.csv")


if __name__ == "__main__":
    main()
