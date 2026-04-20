"""
Task 06 – SQL Basics
Design a wallet transaction database and write queries using
SELECT, WHERE, GROUP BY, ORDER BY, aggregation functions.
"""
import csv, os, sqlite3

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wallet_oltp.db")


# ── helpers ───────────────────────────────────────────────────────────────────
def table(conn: sqlite3.Connection, title: str, sql: str, params=()):
    print(f"\n  [{title}]")
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows), 1)
              for i, c in enumerate(cols)]
    sep   = "  " + "-" * (sum(widths) + 3 * len(widths))
    hdr   = "  " + " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
    print(hdr); print(sep)
    for row in rows[:10]:
        print("  " + " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    if len(rows) > 10:
        print(f"  ... {len(rows)} total rows shown above (top 10 displayed)")


# ── setup DB ──────────────────────────────────────────────────────────────────
print("=" * 58)
print("  Task 06 – SQL Basics: Digital Wallet Database")
print("=" * 58)

conn = sqlite3.connect(DB)
conn.executescript("""
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT UNIQUE,
    city       TEXT,
    join_date  TEXT,
    kyc_status TEXT CHECK(kyc_status IN ('VERIFIED','PENDING','REJECTED'))
);

CREATE TABLE transactions (
    txn_id      TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(user_id),
    merchant    TEXT NOT NULL,
    category    TEXT NOT NULL,
    amount      REAL NOT NULL CHECK(amount > 0),
    status      TEXT NOT NULL CHECK(status IN ('SUCCESS','FAILED','PENDING','REFUNDED')),
    wallet_type TEXT,
    timestamp   TEXT,
    date        TEXT
);

CREATE INDEX idx_txn_user   ON transactions(user_id);
CREATE INDEX idx_txn_date   ON transactions(date);
CREATE INDEX idx_txn_status ON transactions(status);
""")

# load users
with open(os.path.join(BASE, "data", "users.csv")) as f:
    rows = list(csv.DictReader(f))
conn.executemany(
    "INSERT OR IGNORE INTO users VALUES(?,?,?,?,?,?)",
    [(r["user_id"],r["name"],r["email"],r["city"],r["join_date"],r["kyc_status"])
     for r in rows]
)

# load transactions
with open(os.path.join(BASE, "data", "transactions.csv")) as f:
    rows = list(csv.DictReader(f))
conn.executemany(
    "INSERT OR IGNORE INTO transactions VALUES(?,?,?,?,?,?,?,?,?)",
    [(r["txn_id"],r["user_id"],r["merchant"],r["category"],
      float(r["amount"]),r["status"],r["wallet_type"],r["timestamp"],r["date"])
     for r in rows]
)
conn.commit()

u_cnt = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
t_cnt = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
print(f"\n  DB: {os.path.basename(DB)}")
print(f"  Users loaded        : {u_cnt:,}")
print(f"  Transactions loaded : {t_cnt:,}")

# ── Queries ───────────────────────────────────────────────────────────────────
table(conn, "Q1 – SELECT * WHERE amount > 45000",
    """SELECT txn_id, user_id, merchant, amount, status
       FROM transactions WHERE amount > 45000
       ORDER BY amount DESC LIMIT 8""")

table(conn, "Q2 – GROUP BY category: count + total + avg",
    """SELECT category,
              COUNT(*)                          AS txn_count,
              ROUND(SUM(amount), 2)             AS total_inr,
              ROUND(AVG(amount), 2)             AS avg_inr
       FROM transactions
       GROUP BY category
       ORDER BY total_inr DESC""")

table(conn, "Q3 – GROUP BY status: share %",
    """SELECT status,
              COUNT(*)                                                  AS count,
              ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions), 2) AS pct
       FROM transactions
       GROUP BY status ORDER BY count DESC""")

table(conn, "Q4 – Top 5 merchants by revenue",
    """SELECT merchant,
              COUNT(*)             AS txns,
              ROUND(SUM(amount),2) AS revenue
       FROM transactions WHERE status = 'SUCCESS'
       GROUP BY merchant ORDER BY revenue DESC LIMIT 5""")

table(conn, "Q5 – Daily volume (first 7 days)",
    """SELECT date,
              COUNT(*)             AS txns,
              ROUND(SUM(amount),2) AS daily_total
       FROM transactions
       GROUP BY date ORDER BY date LIMIT 7""")

table(conn, "Q6 – WHERE multi-condition: FAILED & high value",
    """SELECT txn_id, user_id, merchant, amount
       FROM transactions
       WHERE status='FAILED' AND amount > 40000
       ORDER BY amount DESC LIMIT 8""")

table(conn, "Q7 – HAVING: users with >50 transactions",
    """SELECT user_id,
              COUNT(*)             AS txn_count,
              ROUND(SUM(amount),2) AS total_spent
       FROM transactions
       GROUP BY user_id HAVING txn_count > 50
       ORDER BY txn_count DESC LIMIT 8""")

table(conn, "Q8 – Wallet-type comparison",
    """SELECT wallet_type,
              COUNT(*) AS txns,
              ROUND(AVG(amount),2) AS avg_txn,
              SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS successes
       FROM transactions
       GROUP BY wallet_type ORDER BY txns DESC""")

conn.close()
print("\nTask 06 complete ✓")
print(f"Database saved: {DB}")
