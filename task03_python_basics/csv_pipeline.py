"""
Task 03 – Python Basics
Read multiple monthly CSV files, clean missing/bad values,
merge datasets, and save a unified clean file.
"""
import csv, os, re
from datetime import datetime

BASE       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR   = os.path.join(BASE, "data")
OUT_DIR    = os.path.dirname(os.path.abspath(__file__))

VALID_STATUSES = {"SUCCESS", "FAILED", "PENDING", "REFUNDED"}
VALID_CATS     = {"Shopping","Food","Transport","Entertainment",
                  "Utilities","Travel","Grocery","Transfer"}

# ── Step 1: Read all monthly CSVs ─────────────────────────────────────────────
def read_csv(path: str) -> list:
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  Read {len(rows):>5,} rows  ←  {os.path.basename(path)}")
    return rows


def load_monthly_files() -> list:
    print("\n[STEP 1] Reading monthly CSV files...")
    files = sorted(
        f for f in os.listdir(DATA_DIR)
        if f.startswith("transactions_2023_") and f.endswith(".csv")
    )
    all_rows = []
    for fname in files:
        all_rows.extend(read_csv(os.path.join(DATA_DIR, fname)))
    print(f"  Total loaded : {len(all_rows):,} rows from {len(files)} files")
    return all_rows


# ── Step 2: Inspect data quality ─────────────────────────────────────────────
def inspect(rows: list):
    print("\n[STEP 2] Data Quality Inspection...")
    fields = list(rows[0].keys()) if rows else []
    null_c = {f: sum(1 for r in rows if not r.get(f, "").strip()) for f in fields}
    print(f"  {'Field':<14} {'Null/Empty':>10}")
    print(f"  {'─'*28}")
    for f in fields:
        flag = " ← needs attention" if null_c[f] > 0 else ""
        print(f"  {f:<14} {null_c[f]:>10}{flag}")


# ── Step 3: Clean data ────────────────────────────────────────────────────────
def clean(rows: list) -> tuple:
    print("\n[STEP 3] Cleaning data...")
    clean_rows, rejected = [], []
    seen_ids = set()

    for row in rows:
        reasons = []

        # Duplicate check
        txn_id = row.get("txn_id", "").strip()
        if txn_id in seen_ids:
            reasons.append("duplicate txn_id")
        else:
            seen_ids.add(txn_id)

        # Amount validation
        raw_amt = re.sub(r"[^\d.]", "", row.get("amount", ""))
        try:
            amount = round(float(raw_amt), 2)
            if amount <= 0:
                reasons.append("non-positive amount")
        except ValueError:
            reasons.append("invalid amount")
            amount = 0.0

        # Status normalise
        status = row.get("status", "").strip().upper()
        if status not in VALID_STATUSES:
            reasons.append(f"unknown status '{status}'")
            status = "UNKNOWN"

        # Category normalise
        category = row.get("category", "").strip()
        if category not in VALID_CATS:
            reasons.append(f"unknown category '{category}'")

        if reasons:
            rejected.append({"row": row, "reasons": reasons})
            continue

        clean_rows.append({
            "txn_id":      txn_id,
            "user_id":     row.get("user_id", "").strip(),
            "merchant":    row.get("merchant", "").strip(),
            "category":    category,
            "amount":      amount,
            "status":      status,
            "wallet_type": row.get("wallet_type", "").strip(),
            "timestamp":   row.get("timestamp", "").strip(),
            "date":        row.get("date", "").strip(),
        })

    print(f"  Input  : {len(rows):,}")
    print(f"  Clean  : {len(clean_rows):,}")
    print(f"  Rejected: {len(rejected):,}")
    return clean_rows, rejected


# ── Step 4: Merge with users ──────────────────────────────────────────────────
def merge_users(txn_rows: list) -> list:
    print("\n[STEP 4] Merging with users.csv...")
    users = {}
    with open(os.path.join(DATA_DIR, "users.csv")) as f:
        for row in csv.DictReader(f):
            users[row["user_id"]] = {"city": row["city"], "kyc": row["kyc_status"]}

    merged, unmatched = [], 0
    for row in txn_rows:
        u = users.get(row["user_id"])
        if u:
            row["city"] = u["city"]
            row["kyc_status"] = u["kyc"]
        else:
            row["city"] = "Unknown"
            row["kyc_status"] = "Unknown"
            unmatched += 1
        merged.append(row)

    print(f"  Matched  : {len(merged) - unmatched:,}")
    print(f"  Unmatched: {unmatched:,}")
    return merged


# ── Step 5: Save ──────────────────────────────────────────────────────────────
def save(rows: list, path: str):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"\n[STEP 5] Saved {len(rows):,} rows  →  {os.path.basename(path)}")


# ── Step 6: Summary stats ─────────────────────────────────────────────────────
def summarise(rows: list):
    print("\n[SUMMARY]")
    amounts = [r["amount"] for r in rows]
    print(f"  Rows           : {len(rows):,}")
    print(f"  Total (INR)    : {sum(amounts):>15,.2f}")
    print(f"  Average (INR)  : {sum(amounts)/len(amounts):>15,.2f}")
    print(f"  Min (INR)      : {min(amounts):>15,.2f}")
    print(f"  Max (INR)      : {max(amounts):>15,.2f}")

    cats = {}
    for r in rows:
        cats[r["category"]] = cats.get(r["category"], 0) + 1
    print("\n  Category breakdown:")
    for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
        bar = "█" * int(cnt / max(cats.values()) * 30)
        print(f"    {cat:<15} {cnt:>5,}  {bar}")


# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  Task 03 – Python Basics: CSV Read / Clean / Merge")
    print("=" * 55)

    raw      = load_monthly_files()
    inspect(raw)
    cleaned, rejected = clean(raw)
    merged   = merge_users(cleaned)
    out_path = os.path.join(OUT_DIR, "merged_clean.csv")
    save(merged, out_path)
    summarise(merged)

    print("\nTask 03 complete ✓")
