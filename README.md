# Payment Reconciliation System

## Overview
This project simulates a payments platform and a bank settlement system, and performs reconciliation to identify discrepancies at month-end.

Since no dataset was provided, synthetic data was generated to model realistic transaction flows and settlement behavior.

---

## Problem
At month-end, all platform transactions should match bank settlements. However, discrepancies occur due to:

- Settlement delays (1–2 days)
- Rounding differences
- Duplicate records
- Missing or orphan transactions

---

## Approach

1. Generate synthetic datasets:
   - Transactions (platform)
   - Settlements (bank)

2. Inject controlled anomalies:
   - Late settlement (cross-month)
   - Rounding mismatch
   - Duplicate settlement
   - Orphan refund

3. Perform reconciliation:
   - Match transactions with settlements
   - Identify discrepancies

---

## Detected Issues

- Missing settlements
- Late settlements (cross-month)
- Amount mismatches
- Duplicate settlements
- Orphan refunds

---

## How to Run

```bash
pip install -r requirements.txt
python run.py