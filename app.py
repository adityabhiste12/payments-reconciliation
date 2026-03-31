import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from payment_reconciliation import generate_transactions, generate_settlements, reconcile

st.title("💳 Payment Reconciliation System")

# Generate data
transactions = generate_transactions()
settlements = generate_settlements(transactions)

st.subheader("📊 Transactions")
st.dataframe(transactions)

st.subheader("🏦 Settlements")
st.dataframe(settlements)

# Run reconciliation
issues = reconcile(transactions, settlements)

st.subheader("🚨 Issues Detected")

for key, df in issues.items():
    st.write(f"### {key.replace('_', ' ').title()}")
    if df.empty:
        st.success("No issues found")
    else:
        st.dataframe(df)
