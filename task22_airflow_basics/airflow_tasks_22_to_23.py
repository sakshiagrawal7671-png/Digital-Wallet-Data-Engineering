"""
Tasks 22 & 23 – Apache Airflow
  22: Create a DAG for the wallet ETL pipeline
  23: Add scheduling, monitoring, and retry mechanisms
"""
import csv, os, sqlite3, time
from collections import defaultdict
from datetime import datetime, timedelta

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXN_CSV = os.path.join(BASE, "data", "transactions.csv")
OLTP_DB = os.path.join(BASE, "task06_sql_basics", "wallet_oltp.db")


# ══════════════════════════════════════════════════════════════
# Mini Airflow framework (no dependency on real Airflow)
# ══════════════════════════════════════════════════════════════
class TaskInstance:
    STATES = ["queued", "running", "success", "failed", "up_for_retry"]

    def __init__(self, task_id: str, retries: int = 1,
                 retry_delay: timedelta = timedelta(seconds=0)):
        self.task_id     = task_id
        self.retries     = retries
        self.retry_delay = retry_delay
        self.state       = "queued"
        self.attempt     = 0
        self.start_ts    = None
        self.end_ts      = None
        self.xcom: dict  = {}

    def run(self, fn, context: dict):
        self.start_ts = datetime.now()
        last_exc      = None
        for attempt in range(self.retries + 1):
            self.attempt = attempt + 1
            self.state   = "running"
            prefix = f"  {'Retry ' + str(attempt) if attempt else 'Attempt 1':>9}"
            try:
                result = fn(context)
                self.state  = "success"
                self.end_ts = datetime.now()
                dur = (self.end_ts - self.start_ts).total_seconds()
                print(f"{prefix} [{self.task_id}] SUCCESS  ({dur:.2f}s)")
                return result
            except Exception as exc:
                last_exc = exc
                print(f"{prefix} [{self.task_id}] FAILED  – {exc}")
                if attempt < self.retries:
                    self.state = "up_for_retry"
        self.state  = "failed"
        self.end_ts = datetime.now()
        raise RuntimeError(f"Task '{self.task_id}' failed after "
                           f"{self.retries + 1} attempt(s): {last_exc}")

    def duration(self) -> float:
        if self.start_ts and self.end_ts:
            return (self.end_ts - self.start_ts).total_seconds()
        return 0.0


class DAG:
    def __init__(self, dag_id: str, schedule_interval: str,
                 start_date: datetime, catchup: bool = False,
                 default_args: dict | None = None):
        self.dag_id           = dag_id
        self.schedule         = schedule_interval
        self.start_date       = start_date
        self.catchup          = catchup
        self.default_args     = default_args or {}
        self._tasks: dict     = {}
        self._deps: dict      = defaultdict(list)   # task_id → [upstream_ids]

    def task(self, task_id: str, upstream=None,
             retries: int | None = None,
             retry_delay: timedelta = timedelta(seconds=0)):
        """Decorator to register a Python function as a DAG task."""
        _retries = retries if retries is not None else self.default_args.get("retries", 0)
        def decorator(fn):
            self._tasks[task_id] = {"fn": fn, "retries": _retries,
                                    "retry_delay": retry_delay}
            ups = upstream or []
            if isinstance(ups, str):
                ups = [ups]
            self._deps[task_id] = ups
            return fn
        return decorator

    def _topological_order(self) -> list:
        order, done, remaining = [], set(), list(self._tasks.keys())
        for _ in range(len(remaining) ** 2 + 1):          # avoid infinite loop
            for t in remaining[:]:
                if all(d in done for d in self._deps[t]):
                    order.append(t); done.add(t); remaining.remove(t)
            if not remaining:
                break
        return order

    def run(self, execution_date: datetime | None = None) -> dict:
        exec_dt  = execution_date or datetime.now()
        context  = {"execution_date": exec_dt, "dag": self, "results": {}}
        dag_start = datetime.now()
        print(f"\n  DAG run  : {self.dag_id}")
        print(f"  Exec date: {exec_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Schedule : {self.schedule}")
        print(f"  {'─'*50}")

        for task_id in self._topological_order():
            cfg = self._tasks[task_id]
            ti  = TaskInstance(task_id, cfg["retries"], cfg["retry_delay"])
            try:
                result = ti.run(cfg["fn"], context)
                context["results"][task_id] = result
            except RuntimeError as exc:
                print(f"\n  DAG FAILED at task '{task_id}': {exc}")
                return context["results"]

        total = (datetime.now() - dag_start).total_seconds()
        print(f"  {'─'*50}")
        print(f"  DAG finished in {total:.2f}s | {len(context['results'])}/{len(self._tasks)} tasks")
        return context["results"]


# ══════════════════════════════════════════════════════════════
# TASK 22 – Basic DAG
# ══════════════════════════════════════════════════════════════
def task22():
    print("=" * 60)
    print("  Task 22 – Airflow Basics: ETL DAG")
    print("=" * 60)
    print("""
  DAG structure:
  check_source ──► extract ──► validate ──► transform ──► load
                                                          │
                                                          ├──► report
                                                          └──► alert
    """)

    dag = DAG(
        dag_id="wallet_daily_etl",
        schedule_interval="0 2 * * *",    # daily at 02:00
        start_date=datetime(2023, 1, 1),
        catchup=False,
        default_args={"retries": 1, "owner": "data-team"},
    )

    # ── task definitions ──────────────────────────────────────

    @dag.task("check_source")
    def check_source(ctx):
        size = os.path.getsize(TXN_CSV) // 1024
        return {"path": TXN_CSV, "size_kb": size}

    @dag.task("extract", upstream="check_source", retries=2)
    def extract(ctx):
        with open(TXN_CSV) as f:
            rows = list(csv.DictReader(f))
        return {"rows": len(rows), "data": rows}

    @dag.task("validate", upstream="extract")
    def validate(ctx):
        data  = ctx["results"]["extract"]["data"]
        valid = [r for r in data
                 if float(r["amount"]) > 0 and r["txn_id"].startswith("TXN")]
        rate  = len(valid) / len(data) * 100
        if rate < 90:
            raise ValueError(f"Validation failed: only {rate:.1f}% valid")
        return {"rows": len(valid), "valid_pct": round(rate, 2), "data": valid}

    @dag.task("transform", upstream="validate")
    def transform(ctx):
        data = ctx["results"]["validate"]["data"]
        out  = []
        for r in data:
            amt = float(r["amount"])
            out.append({**r,
                "gst":        round(amt * 0.18, 2),
                "net_amount": round(amt * 0.82, 2),
                "is_success": 1 if r["status"] == "SUCCESS" else 0,
                "month":      r["date"][:7],
            })
        return {"rows": len(out), "data": out}

    @dag.task("load", upstream="transform", retries=3)
    def load(ctx):
        data = ctx["results"]["transform"]["data"]
        conn = sqlite3.connect(OLTP_DB)
        conn.execute("""CREATE TABLE IF NOT EXISTS airflow_final (
            txn_id TEXT PRIMARY KEY, user_id TEXT, merchant TEXT, category TEXT,
            amount REAL, gst REAL, net_amount REAL, status TEXT, is_success INT,
            wallet_type TEXT, date TEXT, month TEXT)""")
        conn.executemany(
            "INSERT OR REPLACE INTO airflow_final VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            [(r["txn_id"],r["user_id"],r["merchant"],r["category"],float(r["amount"]),
              r["gst"],r["net_amount"],r["status"],r["is_success"],
              r["wallet_type"],r["date"],r["month"]) for r in data]
        )
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM airflow_final").fetchone()[0]
        conn.close()
        return {"rows_loaded": n}

    @dag.task("report", upstream="load")
    def report(ctx):
        n = ctx["results"]["load"]["rows_loaded"]
        print(f"          [Report] {n:,} rows available for analysis")
        return {"report": "daily_summary.json", "rows": n}

    @dag.task("alert", upstream="load")
    def alert(ctx):
        n = ctx["results"]["load"]["rows_loaded"]
        print(f"          [Alert]  Slack #data-team: wallet_daily_etl done – {n:,} rows loaded")
        return {"sent": True}

    results = dag.run(execution_date=datetime(2023, 3, 1))
    print("\nTask 22 complete ✓")
    return dag


# ══════════════════════════════════════════════════════════════
# TASK 23 – Advanced: scheduling, monitoring, retries
# ══════════════════════════════════════════════════════════════
def task23(dag):
    print("\n" + "=" * 60)
    print("  Task 23 – Airflow Advanced: Scheduling & Monitoring")
    print("=" * 60)

    print("""
  [Schedule Expressions]
  @hourly      →  "0 * * * *"      every hour
  @daily       →  "0 0 * * *"      midnight daily
  @weekly      →  "0 0 * * 0"      Sunday midnight
  @monthly     →  "0 0 1 * *"      1st of month
  Custom       →  "0 2 * * 1-5"    weekdays at 02:00

  [Retry Configuration]
  default_args = {
      "retries":          3,
      "retry_delay":      timedelta(minutes=5),
      "retry_exponential_backoff": True,   # 5m → 10m → 20m
      "email_on_failure": True,
      "email_on_retry":   False,
  }

  [Task-level SLA]
  task = PythonOperator(
      ...
      sla=timedelta(hours=1),
      on_failure_callback=send_slack_alert,
      on_success_callback=log_to_datadog,
  )

  [XCom – Cross-task communication]
  Push:  ti.xcom_push(key="row_count", value=100_000)
  Pull:  count = ti.xcom_pull(task_ids="extract", key="row_count")

  [DAG-level monitoring]
  dag_run.state  →  running / success / failed
  dag.sla_miss_callback  →  triggered if DAG breaches SLA
  Airflow UI:   http://localhost:8080
  CLI:          airflow dags trigger wallet_daily_etl
    """)

    # Simulate 7-day schedule history
    print("  [Simulated 7-day Schedule History]")
    print(f"  {'Run Date':<22} {'Duration':>9} {'Status':>9} {'Rows':>8}")
    print(f"  {'─'*54}")
    import random
    random.seed(7)
    for i in range(7):
        run_dt = datetime(2023, 3, i + 1, 2, 0)
        dur    = round(random.uniform(38, 62), 1)
        ok     = random.random() > 0.1
        status = "success" if ok else "failed "
        rows   = f"{random.randint(8100, 8600):,}" if ok else "0"
        print(f"  {run_dt.strftime('%Y-%m-%d %H:%M'):22} {dur:>8.1f}s {status:>9} {rows:>8}")

    print(f"""
  [Task State Machine]
  queued → running → success
                  ↘ failed → up_for_retry → running
                           ↘ failed (max retries exceeded)

  [Backfill – catch up on missed runs]
  $ airflow dags backfill -s 2023-01-01 -e 2023-01-31 wallet_daily_etl
    """)
    print("Task 23 complete ✓")


# ── main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    dag = task22()
    task23(dag)
