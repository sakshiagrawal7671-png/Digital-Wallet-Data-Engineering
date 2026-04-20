"""
Tasks 08–11 combined runner
  08 – Database Concepts: OLTP vs OLAP
  09 – Data Warehousing: Star Schema
  10 – ETL vs ELT pipelines
  11 – Batch Data Ingestion
"""
import csv, os, sqlite3, time, hashlib
from datetime import datetime, date, timedelta

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXN_CSV  = os.path.join(BASE, "data", "transactions.csv")
USER_CSV = os.path.join(BASE, "data", "users.csv")

def hr(n=55): print("─" * n)

# ══════════════════════════════════════════════════════════════
# TASK 08 – OLTP vs OLAP
# ══════════════════════════════════════════════════════════════
def task08():
    print("\n" + "=" * 55)
    print("  Task 08 – Database Concepts: OLTP vs OLAP")
    print("=" * 55)

    comparison = [
        ("Purpose",        "Day-to-day operations",          "Historical analytics & BI"),
        ("Data volume",    "GB (current data)",              "TB–PB (years of history)"),
        ("Query type",     "Simple CRUD, single-row reads",  "Complex GROUP BY, JOINs"),
        ("Latency",        "< 10 ms",                        "Seconds to minutes"),
        ("Schema",         "3NF normalised",                 "Star / Snowflake"),
        ("Updates",        "Frequent INSERT/UPDATE/DELETE",  "Bulk batch loads"),
        ("Concurrency",    "1000s of concurrent users",      "Tens of analysts"),
        ("Tools",          "MySQL, PostgreSQL, SQLite",      "Redshift, BigQuery, Snowflake"),
        ("Wallet example", "Record a payment in real-time",  "Monthly revenue report"),
    ]
    print(f"\n  {'Feature':<16} {'OLTP':<34} {'OLAP'}")
    hr(90)
    for row in comparison:
        print(f"  {row[0]:<16} {row[1]:<34} {row[2]}")

    # Timed OLTP vs OLAP query on same SQLite DB
    db = os.path.join(BASE, "task06_sql_basics", "wallet_oltp.db")
    conn = sqlite3.connect(db)

    t0 = time.perf_counter()
    conn.execute("SELECT * FROM transactions WHERE txn_id='TXN00000001'").fetchone()
    oltp_ms = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    conn.execute("""
        SELECT SUBSTR(date,1,7) AS month, category, COUNT(*), ROUND(SUM(amount),2)
        FROM transactions GROUP BY month, category ORDER BY month""").fetchall()
    olap_ms = (time.perf_counter() - t0) * 1000

    conn.close()
    print(f"\n  OLTP single-row lookup    : {oltp_ms:.3f} ms")
    print(f"  OLAP monthly aggregation  : {olap_ms:.1f} ms")
    print("\nTask 08 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 09 – Star Schema
# ══════════════════════════════════════════════════════════════
def task09():
    print("\n" + "=" * 55)
    print("  Task 09 – Data Warehousing: Star Schema")
    print("=" * 55)

    DW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wallet_dw.db")
    conn = sqlite3.connect(DW)
    conn.executescript("""
    DROP TABLE IF EXISTS fact_transactions;
    DROP TABLE IF EXISTS dim_user; DROP TABLE IF EXISTS dim_merchant;
    DROP TABLE IF EXISTS dim_date; DROP TABLE IF EXISTS dim_category;
    DROP TABLE IF EXISTS dim_wallet;

    CREATE TABLE dim_user (
        user_sk   INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id   TEXT UNIQUE, city TEXT, kyc_status TEXT, city_tier TEXT);
    CREATE TABLE dim_merchant (
        merchant_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE, type TEXT);
    CREATE TABLE dim_date (
        date_sk INTEGER PRIMARY KEY, date TEXT UNIQUE,
        day INT, month INT, month_name TEXT,
        quarter INT, year INT, is_weekend INT);
    CREATE TABLE dim_category (
        cat_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT UNIQUE, parent TEXT);
    CREATE TABLE dim_wallet (
        wallet_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet_type TEXT UNIQUE);
    CREATE TABLE fact_transactions (
        txn_sk      INTEGER PRIMARY KEY AUTOINCREMENT,
        txn_id      TEXT,
        user_sk     INT REFERENCES dim_user(user_sk),
        merchant_sk INT REFERENCES dim_merchant(merchant_sk),
        date_sk     INT REFERENCES dim_date(date_sk),
        cat_sk      INT REFERENCES dim_category(cat_sk),
        wallet_sk   INT REFERENCES dim_wallet(wallet_sk),
        amount      REAL, status TEXT, is_success INT, hour INT);
    """)

    # Dimension data
    city_tier = {"Mumbai":"T1","Delhi":"T1","Bangalore":"T1","Hyderabad":"T1",
                 "Chennai":"T1","Kolkata":"T1","Pune":"T2","Ahmedabad":"T2",
                 "Jaipur":"T2","Ranchi":"T3"}
    merch_type = {"Amazon":"E-Commerce","Flipkart":"E-Commerce","Zomato":"Food",
                  "Swiggy":"Food","Uber":"Transport","Ola":"Transport",
                  "Netflix":"Entertainment","Spotify":"Entertainment",
                  "BigBasket":"Grocery","PhonePe":"Fintech","Paytm":"Fintech",
                  "GooglePay":"Fintech","IRCTC":"Travel","MakeMyTrip":"Travel",
                  "BookMyShow":"Entertainment"}
    cat_parent = {"Shopping":"Retail","Food":"F&B","Transport":"Mobility",
                  "Entertainment":"Leisure","Utilities":"Bills","Travel":"Leisure",
                  "Grocery":"F&B","Transfer":"Financial"}
    months = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    with open(USER_CSV) as f:
        for r in csv.DictReader(f):
            conn.execute("INSERT OR IGNORE INTO dim_user VALUES(NULL,?,?,?,?)",
                (r["user_id"],r["city"],r["kyc_status"],city_tier.get(r["city"],"T3")))
    for m,t in merch_type.items():
        conn.execute("INSERT OR IGNORE INTO dim_merchant VALUES(NULL,?,?)",(m,t))
    for c,p in cat_parent.items():
        conn.execute("INSERT OR IGNORE INTO dim_category VALUES(NULL,?,?)",(c,p))
    for w in ["PayWallet","QuickPay","FastCash","SafePay","EasyWallet"]:
        conn.execute("INSERT OR IGNORE INTO dim_wallet VALUES(NULL,?)",(w,))
    d = date(2023,1,1)
    while d <= date(2023,12,31):
        sk = int(d.strftime("%Y%m%d"))
        conn.execute("INSERT OR IGNORE INTO dim_date VALUES(?,?,?,?,?,?,?,?)",
            (sk,d.strftime("%Y-%m-%d"),d.day,d.month,months[d.month],
             (d.month-1)//3+1, d.year, 1 if d.weekday()>=5 else 0))
        d += timedelta(days=1)
    conn.commit()

    # Load fact table
    u_map = {r[0]:r[1] for r in conn.execute("SELECT user_id,user_sk FROM dim_user")}
    m_map = {r[0]:r[1] for r in conn.execute("SELECT name,merchant_sk FROM dim_merchant")}
    c_map = {r[0]:r[1] for r in conn.execute("SELECT category,cat_sk FROM dim_category")}
    w_map = {r[0]:r[1] for r in conn.execute("SELECT wallet_type,wallet_sk FROM dim_wallet")}

    with open(TXN_CSV) as f:
        facts = []
        for r in csv.DictReader(f):
            ds = int(r["date"].replace("-",""))
            hr = int(r["timestamp"].split(" ")[1].split(":")[0])
            facts.append((r["txn_id"],u_map.get(r["user_id"]),m_map.get(r["merchant"]),
                ds,c_map.get(r["category"]),w_map.get(r["wallet_type"]),
                float(r["amount"]),r["status"],1 if r["status"]=="SUCCESS" else 0,hr))
    conn.executemany(
        "INSERT INTO fact_transactions(txn_id,user_sk,merchant_sk,date_sk,cat_sk,wallet_sk,"
        "amount,status,is_success,hour) VALUES(?,?,?,?,?,?,?,?,?,?)", facts)
    conn.commit()

    n_fact = conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    print(f"\n  Star schema built: fact_transactions = {n_fact:,} rows")
    print(f"  Dimensions: dim_user, dim_merchant, dim_date, dim_category, dim_wallet")

    print("\n  [Sample DW Query – Q1 revenue by city tier]")
    rows = conn.execute("""
        SELECT u.city_tier, u.city,
               COUNT(*) AS txns,
               ROUND(SUM(f.amount)/1e6,2) AS rev_M
        FROM fact_transactions f
        JOIN dim_user u ON f.user_sk=u.user_sk
        JOIN dim_date d ON f.date_sk=d.date_sk
        WHERE d.quarter=1 AND f.is_success=1
        GROUP BY u.city_tier, u.city ORDER BY rev_M DESC LIMIT 8""").fetchall()
    print(f"  {'Tier':<6} {'City':<14} {'Txns':>6} {'Revenue (M)':>12}")
    for r in rows:
        print(f"  {r[0]:<6} {r[1]:<14} {r[2]:>6,} {r[3]:>12.2f}")

    conn.close()
    print(f"\nTask 09 complete ✓  –  DB saved: {DW}")


# ══════════════════════════════════════════════════════════════
# TASK 10 – ETL vs ELT
# ══════════════════════════════════════════════════════════════
def task10():
    print("\n" + "=" * 55)
    print("  Task 10 – ETL vs ELT Pipelines")
    print("=" * 55)

    with open(TXN_CSV) as f:
        raw = list(csv.DictReader(f))

    # ─ ETL ────────────────────────────────────────────────────
    print("\n  [ETL] Extract → Transform → Load")
    t0 = time.perf_counter()

    # Extract
    extracted = raw[:]
    t_ext = time.perf_counter()

    # Transform in Python
    cleaned = []
    for r in extracted:
        amt = float(r["amount"])
        cleaned.append({
            "txn_id": r["txn_id"], "user_id": r["user_id"],
            "merchant": r["merchant"], "category": r["category"],
            "amount": round(amt,2), "gst": round(amt*0.18,2),
            "net": round(amt*0.82,2), "status": r["status"].upper(),
            "is_success": 1 if r["status"]=="SUCCESS" else 0,
            "wallet_type": r["wallet_type"], "date": r["date"],
            "month": r["date"][:7],
        })
    t_tf = time.perf_counter()

    # Load to DB
    etl_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl_output.db")
    conn = sqlite3.connect(etl_db)
    conn.execute("DROP TABLE IF EXISTS clean_txn")
    conn.execute("""CREATE TABLE clean_txn(txn_id,user_id,merchant,category,
        amount,gst,net,status,is_success,wallet_type,date,month)""")
    conn.executemany("INSERT INTO clean_txn VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        [tuple(r.values()) for r in cleaned])
    conn.commit(); conn.close()
    t_load = time.perf_counter()

    etl_ext_ms = (t_ext - t0)*1000
    etl_tf_ms  = (t_tf  - t_ext)*1000
    etl_ld_ms  = (t_load - t_tf)*1000

    print(f"    Extract   : {etl_ext_ms:.1f} ms")
    print(f"    Transform : {etl_tf_ms:.1f} ms  (Python)")
    print(f"    Load      : {etl_ld_ms:.1f} ms")

    # ─ ELT ────────────────────────────────────────────────────
    print("\n  [ELT] Extract → Load (raw) → Transform (SQL)")
    t0 = time.perf_counter()

    elt_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "elt_output.db")
    conn = sqlite3.connect(elt_db)
    conn.execute("DROP TABLE IF EXISTS raw_txn")
    conn.execute("""CREATE TABLE raw_txn(txn_id,user_id,merchant,category,
        amount,status,wallet_type,timestamp,date)""")
    conn.executemany("INSERT INTO raw_txn VALUES(?,?,?,?,?,?,?,?,?)",
        [(r["txn_id"],r["user_id"],r["merchant"],r["category"],r["amount"],
          r["status"],r["wallet_type"],r["timestamp"],r["date"]) for r in raw])
    conn.commit()
    t_load2 = time.perf_counter()

    conn.executescript("""
    DROP TABLE IF EXISTS clean_txn_elt;
    CREATE TABLE clean_txn_elt AS
    SELECT txn_id, user_id, merchant, category,
           ROUND(CAST(amount AS REAL),2)       AS amount,
           ROUND(CAST(amount AS REAL)*0.18,2)  AS gst,
           ROUND(CAST(amount AS REAL)*0.82,2)  AS net,
           UPPER(status)                       AS status,
           CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END AS is_success,
           wallet_type, date,
           SUBSTR(date,1,7) AS month
    FROM raw_txn WHERE CAST(amount AS REAL) > 0;
    """)
    conn.commit(); conn.close()
    t_tf2 = time.perf_counter()

    elt_ld_ms = (t_load2 - t0)*1000
    elt_tf_ms = (t_tf2 - t_load2)*1000

    print(f"    Load (raw): {elt_ld_ms:.1f} ms")
    print(f"    Transform : {elt_tf_ms:.1f} ms  (SQL in DB)")

    # Comparison table
    print(f"\n  {'Metric':<18} {'ETL':>12} {'ELT':>12}")
    hr(45)
    print(f"  {'Extract':18} {etl_ext_ms:>10.1f}ms {0:>10.1f}ms")
    print(f"  {'Transform':18} {etl_tf_ms:>10.1f}ms {elt_tf_ms:>10.1f}ms")
    print(f"  {'Load':18} {etl_ld_ms:>10.1f}ms {elt_ld_ms:>10.1f}ms")
    print(f"  {'Total':18} {etl_ext_ms+etl_tf_ms+etl_ld_ms:>10.1f}ms "
          f"{elt_ld_ms+elt_tf_ms:>10.1f}ms")
    print("\nTask 10 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 11 – Batch Ingestion
# ══════════════════════════════════════════════════════════════
def task11():
    print("\n" + "=" * 55)
    print("  Task 11 – Batch Data Ingestion Pipeline")
    print("=" * 55)

    BATCH   = 5000
    ING_DB  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingestion.db")

    conn = sqlite3.connect(ING_DB)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS ingested(
        txn_id TEXT PRIMARY KEY, user_id TEXT, merchant TEXT,
        category TEXT, amount REAL, status TEXT, wallet_type TEXT,
        date TEXT, batch_id INT, checksum TEXT, loaded_at TEXT);
    CREATE TABLE IF NOT EXISTS batch_log(
        batch_id INT, size INT, ok INT, failed INT,
        duration_ms REAL, loaded_at TEXT);
    """)
    conn.commit()

    print(f"\n  Source   : {os.path.basename(TXN_CSV)}")
    print(f"  Batch sz : {BATCH:,}")

    start  = time.perf_counter()
    batch  = []
    bid    = 0
    total_ok = total_fail = 0

    with open(TXN_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            batch.append(row)
            if len(batch) == BATCH:
                bid += 1
                ok, fail, ms = _ingest_batch(conn, batch, bid)
                total_ok += ok; total_fail += fail
                print(f"  Batch {bid:>2}: {ok:>5,} OK | {fail:>3} fail | {ms:>7.1f} ms")
                batch = []
        if batch:
            bid += 1
            ok, fail, ms = _ingest_batch(conn, batch, bid)
            total_ok += ok; total_fail += fail
            print(f"  Batch {bid:>2}: {ok:>5,} OK | {fail:>3} fail | {ms:>7.1f} ms")

    elapsed = time.perf_counter() - start
    verified = conn.execute("SELECT COUNT(*) FROM ingested").fetchone()[0]
    conn.close()

    print(f"\n  {'─'*40}")
    print(f"  Total loaded  : {total_ok:>8,}")
    print(f"  Total failed  : {total_fail:>8,}")
    print(f"  DB row count  : {verified:>8,}")
    print(f"  Elapsed       : {elapsed:.2f} s")
    print(f"  Throughput    : {total_ok/elapsed:>8,.0f} rows/sec")
    print("\nTask 11 complete ✓")


def _ingest_batch(conn, rows, bid):
    t0 = time.perf_counter()
    ok = fail = 0
    recs = []
    for r in rows:
        try:
            amt = float(r["amount"])
            assert amt > 0 and r["txn_id"].startswith("TXN")
            chk = hashlib.md5(f"{r['txn_id']}{r['amount']}".encode()).hexdigest()[:8]
            recs.append((r["txn_id"],r["user_id"],r["merchant"],r["category"],
                         amt,r["status"],r["wallet_type"],r["date"],bid,chk,
                         datetime.now().isoformat()))
            ok += 1
        except Exception:
            fail += 1
    conn.executemany(
        "INSERT OR IGNORE INTO ingested VALUES(?,?,?,?,?,?,?,?,?,?,?)", recs)
    conn.commit()
    ms = (time.perf_counter()-t0)*1000
    conn.execute(
        "INSERT INTO batch_log VALUES(?,?,?,?,?,?)",
        (bid,len(rows),ok,fail,round(ms,2),datetime.now().isoformat()))
    conn.commit()
    return ok, fail, ms


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    task08()
    task09()
    task10()
    task11()
