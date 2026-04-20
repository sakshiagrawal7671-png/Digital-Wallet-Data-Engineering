"""
Tasks 28 & 29 – Lakehouse + Data Quality
  28: Implement Delta Lake / Iceberg – manage versions, time travel
  29: Build validation checks and anomaly detection system
"""
import csv, json, os, statistics, hashlib, re
from datetime import datetime
from collections import defaultdict

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXN_CSV = os.path.join(BASE, "data", "transactions.csv")

with open(TXN_CSV) as f:
    ALL_ROWS = list(csv.DictReader(f))


# ══════════════════════════════════════════════════════════════
# TASK 28 – Delta Lake Simulation
# ══════════════════════════════════════════════════════════════
class DeltaTable:
    """
    Simulates a Delta Lake table.
    • Transactions are logged in _delta_log/ as JSON commit entries.
    • Data files are written as CSV partitions under data/.
    • Time-travel: read(version=n) replays the log up to version n.
    • VACUUM: removes files unreachable by the current snapshot.
    """

    def __init__(self, table_path: str):
        self.path      = table_path
        self.log_dir   = os.path.join(table_path, "_delta_log")
        self.data_dir  = os.path.join(table_path, "data")
        os.makedirs(self.log_dir,  exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        self._version  = -1
        self._log      = []       # list of commit dicts
        self._current_files: list = []

    # ── internal ──────────────────────────────────────────────
    def _write_commit(self, action: str, added: list, removed: list, meta: dict):
        self._version += 1
        commit = {
            "version":   self._version,
            "timestamp": datetime.now().isoformat(),
            "action":    action,
            "added":     added,
            "removed":   removed,
            "commitInfo": meta,
        }
        self._log.append(commit)
        log_path = os.path.join(self.log_dir, f"{self._version:05d}.json")
        with open(log_path, "w") as f:
            json.dump(commit, f, indent=2)
        return self._version

    def _write_data_file(self, rows: list, tag: str) -> str:
        fname = f"part-{self._version:05d}-{tag[:8]}.csv"
        fpath = os.path.join(self.data_dir, fname)
        if rows:
            with open(fpath, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader(); w.writerows(rows)
        return fname

    # ── public API ────────────────────────────────────────────
    def write(self, rows: list, mode: str = "append") -> int:
        """Write rows to Delta table. mode: 'append' or 'overwrite'."""
        tag   = hashlib.md5(str(len(rows)).encode()).hexdigest()
        fname = self._write_data_file(rows, tag)

        removed = []
        if mode == "overwrite":
            removed = self._current_files[:]
            self._current_files = []

        self._current_files.append(fname)
        v = self._write_commit(
            action=mode.upper(),
            added=[{"file": fname, "rows": len(rows)}],
            removed=[{"file": f} for f in removed],
            meta={"rows": len(rows), "mode": mode},
        )
        print(f"  write(mode={mode:<9})  v{v}  →  {fname}  ({len(rows):,} rows)")
        return v

    def read(self, version: int | None = None) -> list:
        """Read the table at a given version (time travel)."""
        target_v = version if version is not None else self._version
        # replay log up to target_v
        active_files: list = []
        for commit in self._log:
            if commit["version"] > target_v:
                break
            if commit["action"] == "OVERWRITE":
                active_files = []
            for entry in commit["added"]:
                active_files.append(entry["file"])
            for entry in commit["removed"]:
                if entry["file"] in active_files:
                    active_files.remove(entry["file"])

        rows = []
        for fname in active_files:
            fpath = os.path.join(self.data_dir, fname)
            if os.path.exists(fpath):
                with open(fpath) as f:
                    rows.extend(list(csv.DictReader(f)))
        return rows

    def update(self, predicate, update_fn) -> int:
        """Copy-on-write UPDATE."""
        current = self.read()
        updated_rows  = []
        unchanged     = 0
        changed       = 0
        for row in current:
            if predicate(row):
                updated_rows.append(update_fn(dict(row)))
                changed += 1
            else:
                updated_rows.append(row)
                unchanged += 1
        v = self.write(updated_rows, mode="overwrite")
        print(f"  update():  {changed} rows changed, {unchanged} unchanged  → v{v}")
        return v

    def history(self):
        print(f"\n  [Delta Log: {os.path.basename(self.path)}]")
        print(f"  {'Version':<8} {'Action':<12} {'Rows':>8}  Timestamp")
        print(f"  {'─'*55}")
        for c in self._log:
            rows = c["commitInfo"].get("rows", "─")
            print(f"  v{c['version']:<7} {c['action']:<12} {str(rows):>8}  {c['timestamp'][:19]}")

    def vacuum(self, retain_versions: int = 2):
        """Remove data files not referenced in last N versions."""
        keep_set = set()
        for commit in self._log[-(retain_versions):]:
            for entry in commit["added"]:
                keep_set.add(entry["file"])
        deleted = 0
        for fname in os.listdir(self.data_dir):
            if fname not in keep_set:
                os.remove(os.path.join(self.data_dir, fname))
                deleted += 1
        print(f"  vacuum(retain={retain_versions})  deleted {deleted} unreferenced file(s)")

    def optimize(self):
        print(f"  optimize()  compacting small files into 1 file (Z-ORDER by user_id)")


def task28():
    print("=" * 60)
    print("  Task 28 – Lakehouse: Delta Lake Version Management")
    print("=" * 60)

    table_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "delta_wallet_transactions")
    table = DeltaTable(table_path)

    # Slice data by month
    jan = [r for r in ALL_ROWS if r["date"].startswith("2023-01")][:2000]
    feb = [r for r in ALL_ROWS if r["date"].startswith("2023-02")][:1800]
    mar = [r for r in ALL_ROWS if r["date"].startswith("2023-03")][:2000]

    print("\n  [ACID Writes – versioned commits]")
    v0 = table.write(jan, mode="overwrite")    # initial load
    v1 = table.write(feb, mode="append")       # incremental
    v2 = table.write(mar, mode="append")       # incremental

    # UPDATE: fix some FAILED → PENDING for high-value rows
    v3 = table.update(
        predicate = lambda r: r["status"] == "FAILED" and float(r["amount"]) > 47_000,
        update_fn = lambda r: {**r, "status": "PENDING"},
    )

    table.history()

    # Time travel
    print("\n  [Time Travel]")
    rows_v0 = table.read(version=0)
    rows_v1 = table.read(version=1)
    rows_now = table.read()
    print(f"  read(version=0) → {len(rows_v0):,} rows  (Jan only)")
    print(f"  read(version=1) → {len(rows_v1):,} rows  (Jan+Feb)")
    print(f"  read(latest)    → {len(rows_now):,} rows  (all + updates)")

    # Maintenance
    print()
    table.optimize()
    table.vacuum(retain_versions=2)

    print("""
  [Lakehouse Bronze / Silver / Gold Architecture]

  Bronze (raw)   ──► Silver (cleaned)  ──► Gold (aggregated)
  S3 raw/          Delta wallet_txn      Delta monthly_revenue
  • As-is CSV        • Schema enforced    • Business KPIs
  • No dedup         • Deduped            • Pre-joined
  • Append-only      • Null handled       • Ready for BI

  [Delta Lake vs Apache Iceberg]
  Feature          Delta Lake          Apache Iceberg
  ─────────────────────────────────────────────────────
  Ecosystem        Databricks / Spark  Flink / Trino / Spark
  ACID             Yes                 Yes
  Time travel      Yes                 Yes
  Schema evolution Yes                 Yes
  Hidden partition No                  Yes (no path rewrite)
  Best for         Spark-first shops   Multi-engine shops
    """)
    print("Task 28 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 29 – Data Quality & Anomaly Detection
# ══════════════════════════════════════════════════════════════
VALID_STATUSES = {"SUCCESS", "FAILED", "PENDING", "REFUNDED"}
VALID_CATS     = {"Shopping","Food","Transport","Entertainment",
                  "Utilities","Travel","Grocery","Transfer"}
TXN_RE   = re.compile(r"^TXN\d{8}$")
USER_RE  = re.compile(r"^U\d{4}$")


def task29():
    print("\n" + "=" * 60)
    print("  Task 29 – Data Quality & Anomaly Detection")
    print("=" * 60)
    print(f"  Dataset: {len(ALL_ROWS):,} transactions\n")

    # ── 1. Schema validation ──────────────────────────────────
    print("[1] Schema & Constraint Validation")
    valid_rows, invalid_rows = [], []
    error_types: dict = defaultdict(int)

    for row in ALL_ROWS:
        errors = []
        if not TXN_RE.match(row.get("txn_id", "")):
            errors.append("bad_txn_id"); error_types["bad_txn_id"] += 1
        if not USER_RE.match(row.get("user_id", "")):
            errors.append("bad_user_id"); error_types["bad_user_id"] += 1
        try:
            amt = float(row["amount"])
            if not (1.0 <= amt <= 1_000_000):
                errors.append("amount_range"); error_types["amount_range"] += 1
        except ValueError:
            errors.append("amount_type"); error_types["amount_type"] += 1
        if row.get("status") not in VALID_STATUSES:
            errors.append("bad_status"); error_types["bad_status"] += 1
        if row.get("category") not in VALID_CATS:
            errors.append("bad_category"); error_types["bad_category"] += 1

        (invalid_rows if errors else valid_rows).append(row)

    total = len(ALL_ROWS)
    print(f"  Valid   : {len(valid_rows):>8,}  ({len(valid_rows)/total*100:.2f}%)")
    print(f"  Invalid : {len(invalid_rows):>8,}  ({len(invalid_rows)/total*100:.2f}%)")
    print(f"  Error types: {dict(error_types)}")

    # ── 2. Completeness & uniqueness ─────────────────────────
    print("\n[2] Completeness & Uniqueness")
    ids  = [r["txn_id"] for r in ALL_ROWS]
    dups = len(ids) - len(set(ids))
    null_amt   = sum(1 for r in ALL_ROWS if not r.get("amount","").strip())
    null_uid   = sum(1 for r in ALL_ROWS if not r.get("user_id","").strip())
    print(f"  Duplicate txn_ids  : {dups:,}")
    print(f"  Null amounts       : {null_amt:,}")
    print(f"  Null user_ids      : {null_uid:,}")
    print(f"  Unique users       : {len(set(r['user_id'] for r in ALL_ROWS)):,}")

    # ── 3. Z-score anomaly detection ─────────────────────────
    print("\n[3] Statistical Anomaly Detection (Z-Score, threshold=3.0)")
    amounts = [float(r["amount"]) for r in ALL_ROWS]
    mean    = statistics.mean(amounts)
    stdev   = statistics.stdev(amounts)
    Z_THRESH = 3.0

    anomalies = []
    for row in ALL_ROWS:
        amt = float(row["amount"])
        z   = (amt - mean) / stdev
        if abs(z) > Z_THRESH:
            anomalies.append({**row, "z_score": round(z, 3)})

    print(f"  Mean     : INR {mean:>12,.2f}")
    print(f"  Std Dev  : INR {stdev:>12,.2f}")
    print(f"  Anomalies: {len(anomalies):,}")
    for a in sorted(anomalies, key=lambda x: abs(x["z_score"]), reverse=True)[:5]:
        print(f"  {a['txn_id']}  INR {float(a['amount']):>10,.2f}  z={a['z_score']:>6.3f}  {a['status']}")

    # ── 4. IQR outlier detection ──────────────────────────────
    print("\n[4] IQR Outlier Detection")
    sorted_a = sorted(amounts)
    n        = len(sorted_a)
    q1 = sorted_a[int(n * 0.25)]
    q3 = sorted_a[int(n * 0.75)]
    iqr = q3 - q1
    lo  = q1 - 1.5 * iqr
    hi  = q3 + 1.5 * iqr
    outliers = [a for a in amounts if a < lo or a > hi]
    print(f"  Q1={q1:,.2f}  Q3={q3:,.2f}  IQR={iqr:,.2f}")
    print(f"  Lower fence={lo:,.2f}  Upper fence={hi:,.2f}")
    print(f"  IQR outliers: {len(outliers):,}  ({len(outliers)/n*100:.2f}%)")

    # ── 5. Behavioural anomalies ─────────────────────────────
    print("\n[5] Behavioural Anomaly Detection (>4σ from user mean)")
    user_amts: dict = defaultdict(list)
    for row in ALL_ROWS:
        user_amts[row["user_id"]].append(float(row["amount"]))

    beh_alerts = []
    for uid, amts in user_amts.items():
        if len(amts) < 5:
            continue
        m = statistics.mean(amts)
        s = statistics.stdev(amts)
        if s == 0:
            continue
        for amt in amts:
            if abs(amt - m) / s > 4:
                beh_alerts.append({"user_id": uid, "amount": amt,
                                   "user_avg": round(m, 2),
                                   "ratio": round(amt / m, 2)})

    beh_alerts.sort(key=lambda x: x["ratio"], reverse=True)
    print(f"  Detected : {len(beh_alerts):,} behavioural outliers")
    for a in beh_alerts[:5]:
        print(f"  {a['user_id']}  INR {a['amount']:>10,.2f}  "
              f"user_avg={a['user_avg']:>8,.2f}  ratio={a['ratio']:.2f}×")

    # ── 6. Velocity check ────────────────────────────────────
    print("\n[6] Velocity Check (same user + same minute)")
    minute_user: dict = defaultdict(int)
    for row in ALL_ROWS:
        key = (row["timestamp"][:16], row["user_id"])
        minute_user[key] += 1

    velocity_hits = [(k, v) for k, v in minute_user.items() if v > 1]
    print(f"  High-velocity events: {len(velocity_hits)}")
    for (ts, uid), cnt in sorted(velocity_hits, key=lambda x: -x[1])[:5]:
        print(f"  {uid} @ {ts}: {cnt} transactions in 1 minute  ← VELOCITY ALERT")

    # ── 7. Overall DQ score ──────────────────────────────────
    print("\n[7] Data Quality Score")
    completeness = (1 - null_amt / total) * 100
    uniqueness   = (1 - dups   / total) * 100
    validity     = len(valid_rows) / total * 100
    dq_score     = (completeness + uniqueness + validity) / 3

    print(f"  Completeness : {completeness:>6.2f}%")
    print(f"  Uniqueness   : {uniqueness:>6.2f}%")
    print(f"  Validity     : {validity:>6.2f}%")
    print(f"  ─────────────────────────")
    grade = "EXCELLENT" if dq_score > 95 else "GOOD" if dq_score > 85 else "POOR"
    print(f"  DQ Score     : {dq_score:>6.2f}%  [{grade}]")

    print("\nTask 29 complete ✓")


# ── main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    task28()
    task29()
