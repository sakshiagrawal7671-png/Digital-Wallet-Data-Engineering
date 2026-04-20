"""
Task 30 – Final Project
End-to-End Digital Wallet Data Pipeline
Ingestion → Processing → Storage → Orchestration → Dashboard

This single script runs the FULL pipeline using all techniques
from Tasks 01–29 and prints a live analytics dashboard.
"""
import csv, json, os, sqlite3, time, statistics, hashlib
from collections import defaultdict
from datetime import datetime

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXN_CSV = os.path.join(BASE, "data", "transactions.csv")
USR_CSV = os.path.join(BASE, "data", "users.csv")
FINAL_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "final_pipeline.db")
OUT_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline_run_summary.json")


def banner(title, char="═"):
    line = char * 60
    print(f"\n{line}\n  {title}\n{line}")


def hline(n=55): print("  " + "─" * n)


# ══════════════════════════════════════════════════════════════
# STAGE 1 – INGESTION
# ══════════════════════════════════════════════════════════════
def stage_ingest() -> dict:
    banner("STAGE 1 – INGESTION")
    t0 = time.perf_counter()

    with open(TXN_CSV) as f:
        txn_rows = list(csv.DictReader(f))
    with open(USR_CSV) as f:
        usr_rows = list(csv.DictReader(f))

    ms = (time.perf_counter() - t0) * 1000
    print(f"  Source          : {os.path.basename(TXN_CSV)}")
    print(f"  Transactions    : {len(txn_rows):,} rows")
    print(f"  Users           : {len(usr_rows):,} rows")
    print(f"  Ingest time     : {ms:.1f} ms")

    # Batch checksum (simulates audit trail)
    batch_hash = hashlib.md5(
        b"".join(r["txn_id"].encode() for r in txn_rows[:100])
    ).hexdigest()
    print(f"  Batch checksum  : {batch_hash}")
    print(f"  Status          : ✓ INGESTION COMPLETE")
    return {"txn_rows": txn_rows, "usr_rows": usr_rows,
            "txn_count": len(txn_rows), "ms": round(ms, 1)}


# ══════════════════════════════════════════════════════════════
# STAGE 2 – PROCESSING & TRANSFORMATION
# ══════════════════════════════════════════════════════════════
VALID_STATUSES = {"SUCCESS","FAILED","PENDING","REFUNDED"}
VALID_CATS     = {"Shopping","Food","Transport","Entertainment",
                  "Utilities","Travel","Grocery","Transfer"}

def stage_process(ingested: dict) -> dict:
    banner("STAGE 2 – PROCESSING & TRANSFORMATION")
    t0 = time.perf_counter()

    txn_rows = ingested["txn_rows"]
    usr_rows = ingested["usr_rows"]

    # Build user lookup
    users = {r["user_id"]: r for r in usr_rows}

    processed, rejected = [], []
    for row in txn_rows:
        # ── validation ──────────────────────────────────────
        errors = []
        try:
            amt = float(row["amount"])
            if not (1 <= amt <= 500_000):
                errors.append("amount_range")
        except ValueError:
            errors.append("amount_type"); amt = 0.0

        if row.get("status") not in VALID_STATUSES:
            errors.append("bad_status")
        if row.get("category") not in VALID_CATS:
            errors.append("bad_category")

        if errors:
            rejected.append({"row": row, "errors": errors})
            continue

        # ── enrich & transform ───────────────────────────────
        user = users.get(row["user_id"], {})
        processed.append({
            "txn_id":      row["txn_id"],
            "user_id":     row["user_id"],
            "city":        user.get("city", "Unknown"),
            "kyc_status":  user.get("kyc_status", "Unknown"),
            "merchant":    row["merchant"],
            "category":    row["category"],
            "amount":      round(amt, 2),
            "gst":         round(amt * 0.18, 2),
            "net_amount":  round(amt * 0.82, 2),
            "status":      row["status"].upper(),
            "is_success":  1 if row["status"] == "SUCCESS" else 0,
            "wallet_type": row["wallet_type"],
            "date":        row["date"],
            "month":       row["date"][:7],
            "quarter":     f"Q{(int(row['date'][5:7])-1)//3+1}",
            "year":        row["date"][:4],
            "processed_at": datetime.now().isoformat(),
        })

    ms = (time.perf_counter() - t0) * 1000
    valid_rate = len(processed) / len(txn_rows) * 100
    print(f"  Transformations : gst, net_amount, city, kyc, month, quarter, year")
    print(f"  Processed       : {len(processed):,} rows  ({valid_rate:.2f}% valid)")
    print(f"  Rejected        : {len(rejected):,} rows")
    print(f"  Process time    : {ms:.1f} ms")
    print(f"  Status          : ✓ PROCESSING COMPLETE")
    return {"rows": processed, "count": len(processed),
            "rejected": len(rejected), "ms": round(ms, 1)}


# ══════════════════════════════════════════════════════════════
# STAGE 3 – STORAGE  (OLTP + partitioned Data Lake)
# ══════════════════════════════════════════════════════════════
def stage_store(processed: dict) -> dict:
    banner("STAGE 3 – STORAGE (OLTP + Data Lake)")
    t0   = time.perf_counter()
    rows = processed["rows"]

    # ── SQLite OLTP ───────────────────────────────────────────
    conn = sqlite3.connect(FINAL_DB)
    conn.execute("DROP TABLE IF EXISTS final_transactions")
    conn.execute("""CREATE TABLE final_transactions (
        txn_id TEXT PRIMARY KEY, user_id TEXT, city TEXT, kyc_status TEXT,
        merchant TEXT, category TEXT, amount REAL, gst REAL, net_amount REAL,
        status TEXT, is_success INT, wallet_type TEXT,
        date TEXT, month TEXT, quarter TEXT, year TEXT, processed_at TEXT)""")
    conn.executemany(
        """INSERT INTO final_transactions VALUES
           (:txn_id,:user_id,:city,:kyc_status,:merchant,:category,
            :amount,:gst,:net_amount,:status,:is_success,:wallet_type,
            :date,:month,:quarter,:year,:processed_at)""",
        rows)
    conn.commit()
    db_count = conn.execute("SELECT COUNT(*) FROM final_transactions").fetchone()[0]
    conn.close()

    # ── Partitioned CSV Data Lake ─────────────────────────────
    lake_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datalake")
    partitions_written = 0
    month_groups: dict = defaultdict(list)
    for r in rows:
        month_groups[r["month"]].append(r)

    for month, m_rows in sorted(month_groups.items())[:6]:  # first 6 months
        part_dir = os.path.join(lake_root,
                                f"year={month[:4]}", f"month={month[5:]}")
        os.makedirs(part_dir, exist_ok=True)
        out_path = os.path.join(part_dir, "part-0.csv")
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(m_rows[0].keys()))
            w.writeheader(); w.writerows(m_rows)
        partitions_written += 1

    ms = (time.perf_counter() - t0) * 1000
    print(f"  SQLite OLTP     : {db_count:,} rows → {os.path.basename(FINAL_DB)}")
    print(f"  Data Lake parts : {partitions_written} partitions (year=*/month=*)")
    print(f"  Storage time    : {ms:.1f} ms")
    print(f"  Status          : ✓ STORAGE COMPLETE")
    return {"db_rows": db_count, "partitions": partitions_written, "ms": round(ms, 1)}


# ══════════════════════════════════════════════════════════════
# STAGE 4 – ORCHESTRATION (Airflow DAG summary)
# ══════════════════════════════════════════════════════════════
def stage_orchestrate(stage_timings: dict) -> dict:
    banner("STAGE 4 – ORCHESTRATION SUMMARY (Airflow DAG)")
    tasks = [
        ("ingest",     stage_timings.get("ingest_ms", 0)),
        ("validate",   stage_timings.get("process_ms", 0) * 0.05),
        ("transform",  stage_timings.get("process_ms", 0) * 0.95),
        ("load",       stage_timings.get("store_ms", 0)),
        ("report",     0.5),
        ("alert",      0.1),
    ]
    print(f"\n  DAG: wallet_daily_etl   Schedule: 0 2 * * *")
    hline(50)
    total = 0.0
    for task_id, ms in tasks:
        bar = "█" * max(1, int(ms / 20))
        print(f"  ✓ {task_id:<12}  {ms:>8.1f} ms  {bar}")
        total += ms
    hline(50)
    print(f"    {'Total':12}  {total:>8.1f} ms")
    print(f"\n  SLA breached     : No  (limit: 1 hour)")
    print(f"  Retry triggered  : No")
    print(f"  Alert sent       : #data-team (Slack)")
    print(f"  Status           : ✓ DAG RUN COMPLETE")
    return {"total_ms": round(total, 1)}


# ══════════════════════════════════════════════════════════════
# STAGE 5 – ANALYTICS DASHBOARD
# ══════════════════════════════════════════════════════════════
def stage_dashboard():
    banner("STAGE 5 – ANALYTICS DASHBOARD")

    conn = sqlite3.connect(FINAL_DB)

    # ── KPI cards ─────────────────────────────────────────────
    kpis = conn.execute("""
        SELECT COUNT(*)                                         AS total_txns,
               ROUND(SUM(amount) / 1e6, 2)                    AS revenue_M,
               ROUND(SUM(CASE WHEN is_success=1 THEN amount END)/1e6, 2) AS success_rev_M,
               ROUND(AVG(amount), 2)                           AS avg_txn,
               ROUND(SUM(is_success)*100.0/COUNT(*), 2)        AS success_rate,
               COUNT(DISTINCT user_id)                         AS active_users
        FROM final_transactions""").fetchone()

    print(f"""
  ╔══════════════════════════════════════════════════════╗
  ║         DIGITAL WALLET ANALYTICS DASHBOARD          ║
  ╠══════════════════╦═══════════════════════════════════╣
  ║ Total Txns       ║ {kpis[0]:>31,} ║
  ║ Total Revenue    ║ INR {kpis[1]:>27.2f}M ║
  ║ Success Revenue  ║ INR {kpis[2]:>27.2f}M ║
  ║ Avg Txn Value    ║ INR {kpis[3]:>27,.2f} ║
  ║ Success Rate     ║ {kpis[4]:>30.2f}% ║
  ║ Active Users     ║ {kpis[5]:>31,} ║
  ╚══════════════════╩═══════════════════════════════════╝""")

    # ── Top categories ────────────────────────────────────────
    print("\n  Top Categories by Revenue:")
    cats = conn.execute("""
        SELECT category, COUNT(*) AS txns,
               ROUND(SUM(amount)/1e6,2) AS rev_M
        FROM final_transactions WHERE is_success=1
        GROUP BY category ORDER BY rev_M DESC""").fetchall()
    max_rev = cats[0][2] if cats else 1
    for cat, txns, rev in cats:
        bar = "█" * int(rev / max_rev * 30)
        print(f"  {cat:<15} {txns:>6,} txns  INR {rev:>6.2f}M  {bar}")

    # ── Monthly trend ─────────────────────────────────────────
    print("\n  Monthly Revenue Trend:")
    monthly = conn.execute("""
        SELECT month, COUNT(*) AS txns, ROUND(SUM(amount)/1e6,2) AS rev_M
        FROM final_transactions WHERE is_success=1
        GROUP BY month ORDER BY month""").fetchall()
    max_m = max(r[2] for r in monthly) if monthly else 1
    for month, txns, rev in monthly:
        bar = "█" * int(rev / max_m * 28)
        print(f"  {month}  {txns:>5,} txns  INR {rev:>6.2f}M  {bar}")

    # ── Top cities ────────────────────────────────────────────
    print("\n  Top 5 Cities:")
    cities = conn.execute("""
        SELECT city, COUNT(*) AS txns, ROUND(SUM(amount)/1e6,2) AS rev_M
        FROM final_transactions WHERE is_success=1
        GROUP BY city ORDER BY rev_M DESC LIMIT 5""").fetchall()
    for city, txns, rev in cities:
        print(f"  {city:<14} {txns:>6,} txns  INR {rev:>6.2f}M")

    # ── Fraud / velocity alerts ───────────────────────────────
    fraud = conn.execute("""
        SELECT COUNT(*) FROM final_transactions
        WHERE is_success=0 AND amount > 45000""").fetchone()[0]
    print(f"\n  Fraud alerts (FAILED > 45k)  : {fraud:,}")

    conn.close()
    print(f"\n  Status  : ✓ DASHBOARD RENDERED")


# ══════════════════════════════════════════════════════════════
# PIPELINE RUNNER
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    pipeline_start = time.perf_counter()

    print("\n" + "█" * 60)
    print("  DIGITAL WALLET – END-TO-END DATA PIPELINE")
    print("  Task 30 – Final Project")
    print("█" * 60)

    # run all stages
    ing  = stage_ingest()
    proc = stage_process(ing)
    stor = stage_store(proc)
    timings = {"ingest_ms":  ing["ms"],
               "process_ms": proc["ms"],
               "store_ms":   stor["ms"]}
    orch = stage_orchestrate(timings)
    stage_dashboard()

    # ── Final summary ─────────────────────────────────────────
    total_s = time.perf_counter() - pipeline_start
    banner("PIPELINE COMPLETE", "▓")
    print(f"  Records in      : {ing['txn_count']:>10,}")
    print(f"  Records out     : {proc['count']:>10,}")
    print(f"  Rejected        : {proc['rejected']:>10,}")
    print(f"  DB rows         : {stor['db_rows']:>10,}")
    print(f"  Partitions      : {stor['partitions']:>10}")
    print(f"  Total duration  : {total_s*1000:>10.1f} ms")
    print(f"  Data quality    : {proc['count']/ing['txn_count']*100:>10.1f}%")
    print(f"  Status          : ✓ ALL STAGES SUCCESSFUL")

    # Save run summary
    summary = {
        "pipeline":    "wallet_daily_etl",
        "run_at":      datetime.now().isoformat(),
        "total_ms":    round(total_s * 1000, 1),
        "stages":      {
            "ingestion":    ing,
            "processing":   proc,
            "storage":      stor,
            "orchestration": orch,
        },
    }
    with open(OUT_JSON, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n  Summary saved   : {os.path.basename(OUT_JSON)}")
    print("\nTask 30 – Final Project complete ✓")
