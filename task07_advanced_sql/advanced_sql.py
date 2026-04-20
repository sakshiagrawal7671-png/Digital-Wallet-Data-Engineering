"""
Task 07 – Advanced SQL
Joins, subqueries (scalar & correlated), CTEs,
and window functions for business reporting.
"""
import os, sqlite3

DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  "task06_sql_basics", "wallet_oltp.db")


def show(conn, title, sql):
    print(f"\n  ── {title} ──")
    cur  = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    w    = [max(len(str(c)), *(len(str(r[i])) for r in rows), 1)
            for i, c in enumerate(cols)]
    print("  " + " | ".join(str(c).ljust(w[i]) for i, c in enumerate(cols)))
    print("  " + "─" * (sum(w) + 3*len(w)))
    for row in rows[:10]:
        print("  " + " | ".join(str(v).ljust(w[i]) for i, v in enumerate(row)))
    if len(rows) > 10:
        print(f"  … {len(rows)} total rows")


conn = sqlite3.connect(DB)
print("=" * 60)
print("  Task 07 – Advanced SQL: Joins / Subqueries / Windows")
print("=" * 60)

# ── INNER JOIN ────────────────────────────────────────────────────────────────
show(conn, "INNER JOIN – transaction with user city & KYC", """
SELECT t.txn_id, t.user_id, u.city, u.kyc_status,
       t.merchant, ROUND(t.amount,2) AS amount, t.status
FROM   transactions t
JOIN   users u ON t.user_id = u.user_id
WHERE  t.status = 'SUCCESS' AND t.amount > 47000
ORDER  BY t.amount DESC
LIMIT  8""")

# ── LEFT JOIN ─────────────────────────────────────────────────────────────────
show(conn, "LEFT JOIN – users with txn count (incl. 0)", """
SELECT u.user_id, u.city, u.kyc_status,
       COUNT(t.txn_id)            AS txn_count,
       ROUND(COALESCE(SUM(t.amount),0),2) AS total_spent
FROM   users u
LEFT JOIN transactions t ON u.user_id = t.user_id
GROUP  BY u.user_id, u.city, u.kyc_status
ORDER  BY txn_count DESC LIMIT 8""")

# ── Scalar subquery ───────────────────────────────────────────────────────────
show(conn, "Scalar subquery – users above average spend", """
SELECT user_id, ROUND(SUM(amount),2) AS total_spent
FROM   transactions
GROUP  BY user_id
HAVING SUM(amount) > (
    SELECT AVG(user_total)
    FROM   (SELECT SUM(amount) AS user_total
            FROM   transactions GROUP BY user_id)
)
ORDER  BY total_spent DESC LIMIT 8""")

# ── Correlated subquery ───────────────────────────────────────────────────────
show(conn, "Correlated subquery – txn vs user avg", """
SELECT t.txn_id, t.user_id, ROUND(t.amount,2) AS amount,
       ROUND((SELECT AVG(t2.amount) FROM transactions t2
              WHERE  t2.user_id = t.user_id), 2) AS user_avg,
       CASE WHEN t.amount >
                 (SELECT AVG(t2.amount) FROM transactions t2
                  WHERE  t2.user_id = t.user_id)
            THEN 'ABOVE_AVG' ELSE 'BELOW_AVG' END AS vs_avg
FROM   transactions t
WHERE  t.status = 'SUCCESS'
ORDER  BY t.amount DESC LIMIT 6""")

# ── CTE ───────────────────────────────────────────────────────────────────────
show(conn, "CTE – monthly revenue summary", """
WITH monthly AS (
    SELECT SUBSTR(date,1,7)      AS month,
           COUNT(*)               AS txns,
           ROUND(SUM(amount),2)   AS revenue,
           ROUND(AVG(amount),2)   AS avg_txn
    FROM   transactions
    WHERE  status = 'SUCCESS'
    GROUP  BY month
)
SELECT month, txns, revenue, avg_txn,
       ROUND(revenue * 100.0 / SUM(revenue) OVER (), 2) AS pct_of_year
FROM   monthly
ORDER  BY month""")

# ── WINDOW: RANK ──────────────────────────────────────────────────────────────
show(conn, "WINDOW RANK – top txn per category", """
SELECT category, txn_id, ROUND(amount,2) AS amount,
       RANK() OVER (PARTITION BY category ORDER BY amount DESC) AS rnk
FROM   transactions
WHERE  status = 'SUCCESS'
ORDER  BY category, rnk LIMIT 12""")

# ── WINDOW: running total ─────────────────────────────────────────────────────
show(conn, "WINDOW SUM – running daily total", """
SELECT date,
       COUNT(*)              AS daily_txns,
       ROUND(SUM(amount),2)  AS daily_rev,
       ROUND(SUM(SUM(amount)) OVER (ORDER BY date
             ROWS UNBOUNDED PRECEDING), 2) AS running_total
FROM   transactions
GROUP  BY date ORDER BY date LIMIT 10""")

# ── WINDOW: LAG ───────────────────────────────────────────────────────────────
show(conn, "WINDOW LAG – day-over-day change", """
WITH daily AS (
    SELECT date, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS rev
    FROM   transactions GROUP BY date
)
SELECT date, cnt, rev,
       LAG(cnt) OVER (ORDER BY date)  AS prev_cnt,
       cnt - LAG(cnt) OVER (ORDER BY date) AS delta,
       ROUND((cnt - LAG(cnt) OVER (ORDER BY date)) * 100.0
             / NULLIF(LAG(cnt) OVER (ORDER BY date), 0), 2) AS pct_chg
FROM   daily ORDER BY date LIMIT 10""")

# ── Fraud detection CTE ───────────────────────────────────────────────────────
show(conn, "CTE + subquery – potential fraud (amount > 3× user avg)", """
WITH user_avg AS (
    SELECT user_id, AVG(amount) AS avg_amt
    FROM   transactions GROUP BY user_id
)
SELECT t.txn_id, t.user_id, ROUND(t.amount,2) AS amount,
       ROUND(u.avg_amt,2) AS user_avg,
       ROUND(t.amount / u.avg_amt, 2) AS ratio,
       'FRAUD_ALERT' AS flag
FROM   transactions t
JOIN   user_avg u ON t.user_id = u.user_id
WHERE  t.amount > u.avg_amt * 3
ORDER  BY ratio DESC LIMIT 8""")

conn.close()
print("\nTask 07 complete ✓")
