"""
Digital Wallet Transaction Dataset Generator
Generates 100,000 realistic transactions for the Data Engineering project.
Run this first before executing any task scripts.
"""
import csv, random, os
from datetime import datetime, timedelta

random.seed(42)

USERS     = [f"U{str(i).zfill(4)}" for i in range(1, 1001)]
MERCHANTS = ["Amazon","Flipkart","Zomato","Swiggy","Uber","Ola",
             "Netflix","Spotify","BigBasket","PhonePe","Paytm",
             "GooglePay","IRCTC","MakeMyTrip","BookMyShow"]
CATEGORIES = ["Shopping","Food","Transport","Entertainment",
              "Utilities","Travel","Grocery","Transfer"]
STATUSES   = ["SUCCESS","FAILED","PENDING","REFUNDED"]
WALLETS    = ["PayWallet","QuickPay","FastCash","SafePay","EasyWallet"]
CITIES     = ["Mumbai","Delhi","Bangalore","Hyderabad","Chennai",
              "Kolkata","Pune","Ranchi","Jaipur","Ahmedabad"]

BASE = os.path.dirname(os.path.abspath(__file__))

def generate():
    print("Generating synthetic dataset...")
    base_date = datetime(2023, 1, 1)

    # ── transactions.csv (100 000 rows) ──────────────────────────────
    txn_rows = []
    for i in range(100_000):
        ts = base_date + timedelta(seconds=random.randint(0, 365*24*3600))
        txn_rows.append([
            f"TXN{str(i+1).zfill(8)}",
            random.choice(USERS),
            random.choice(MERCHANTS),
            random.choice(CATEGORIES),
            round(random.uniform(10, 50_000), 2),
            random.choices(STATUSES, weights=[75, 10, 10, 5])[0],
            random.choice(WALLETS),
            ts.strftime("%Y-%m-%d %H:%M:%S"),
            ts.strftime("%Y-%m-%d"),
        ])

    txn_header = ["txn_id","user_id","merchant","category","amount",
                  "status","wallet_type","timestamp","date"]
    with open(f"{BASE}/transactions.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(txn_header); w.writerows(txn_rows)
    print(f"  transactions.csv  → {len(txn_rows):,} rows")

    # ── monthly slices (Jan–Mar) ──────────────────────────────────────
    for mo in range(1, 4):
        tag = f"2023-{str(mo).zfill(2)}"
        subset = [r for r in txn_rows if r[8].startswith(tag)][:3000]
        path = f"{BASE}/transactions_{tag.replace('-','_')}.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f); w.writerow(txn_header); w.writerows(subset)
        print(f"  transactions_{tag.replace('-','_')}.csv → {len(subset):,} rows")

    # ── users.csv ─────────────────────────────────────────────────────
    user_rows = []
    for uid in USERS:
        jd = (datetime(2020,1,1) + timedelta(days=random.randint(0,1095))).strftime("%Y-%m-%d")
        user_rows.append([uid, f"User_{uid}", f"{uid.lower()}@email.com",
                          random.choice(CITIES), jd,
                          random.choice(["VERIFIED","PENDING","REJECTED"])])
    with open(f"{BASE}/users.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user_id","name","email","city","join_date","kyc_status"])
        w.writerows(user_rows)
    print(f"  users.csv         → {len(user_rows):,} rows")

    print("Data generation complete.\n")

if __name__ == "__main__":
    generate()
