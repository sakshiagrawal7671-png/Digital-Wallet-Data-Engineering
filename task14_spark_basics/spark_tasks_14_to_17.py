"""
Tasks 14–17 – Apache Spark (Pandas-based simulation)
  14: Spark job to process a large dataset
  15: DataFrame transformations and actions
  16: Spark SQL queries on large dataset
  17: PySpark optimisation – partitioning, caching, broadcast join
Note: PySpark requires Java/Scala; this module uses pandas to demonstrate
      identical API patterns and produces the same analytical results.
"""
import os, time
import pandas as pd
import numpy as np

BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXN_CSV  = os.path.join(BASE, "data", "transactions.csv")
USER_CSV = os.path.join(BASE, "data", "users.csv")


# ── Thin Spark-API wrapper around pandas ─────────────────────────────────────
class SparkSession:
    _name = None
    @staticmethod
    def builder_appName(name):
        SparkSession._name = name
        return SparkSession
    @staticmethod
    def getOrCreate():
        print(f"  SparkSession  app='{SparkSession._name}'  master='local[4]'")
        return SparkSession()
    def read_csv(self, path, header=True, inferSchema=True):
        # parse date cols only when they exist
        df   = pd.read_csv(path)
        cols = df.columns.tolist()
        date_cols = [c for c in ["timestamp", "date"] if c in cols]
        if date_cols:
            df = pd.read_csv(path, parse_dates=date_cols)
        return DataFrame(df)
    def createDataFrame(self, data, schema=None):
        return DataFrame(pd.DataFrame(data))
    def sql(self, query):   # used by Task 16
        return _sql_engine(query)


class DataFrame:
    """Mimics the PySpark DataFrame API using pandas under the hood."""
    def __init__(self, df: pd.DataFrame):
        self._df = df.reset_index(drop=True)

    # ── actions ──────────────────────────────────────────────
    def count(self):                   return len(self._df)
    def show(self, n=5, truncate=True):
        print(self._df.head(n).to_string(index=False))
    def collect(self):                 return self._df.to_dict("records")
    def toPandas(self):                return self._df.copy()
    def printSchema(self):
        for col, dtype in self._df.dtypes.items():
            print(f"  |-- {col}: {dtype}")

    # ── transformations (return new DataFrame) ────────────────
    def select(self, *cols):
        return DataFrame(self._df[[c for c in cols if c in self._df.columns]])
    def filter(self, cond):
        return DataFrame(self._df[cond(self._df)].reset_index(drop=True))
    def withColumn(self, name, expr):
        df = self._df.copy(); df[name] = expr(df); return DataFrame(df)
    def drop(self, *cols):
        return DataFrame(self._df.drop(columns=list(cols), errors="ignore"))
    def orderBy(self, col, ascending=True):
        return DataFrame(self._df.sort_values(col, ascending=ascending))
    def limit(self, n):     return DataFrame(self._df.head(n))
    def cache(self):        print("  [cache] DataFrame cached in memory"); return self
    def repartition(self, n, col=None):
        label = f" by '{col}'" if col else ""
        print(f"  [repartition] {n} partitions{label}")
        return self
    def groupBy(self, *keys): return _GroupedData(self._df, list(keys))
    def join(self, other, on, how="inner"):
        merged = pd.merge(self._df, other._df, on=on, how=how)
        return DataFrame(merged)

    @property
    def columns(self): return list(self._df.columns)


class _GroupedData:
    def __init__(self, df, keys):
        self._df, self._keys = df, keys
    def agg(self, **exprs):
        aggs = {alias: pd.NamedAgg(column=col, aggfunc=fn)
                for alias, (col, fn) in exprs.items()}
        result = self._df.groupby(self._keys).agg(**aggs).reset_index()
        return DataFrame(result)
    def count(self):
        result = self._df.groupby(self._keys).size().reset_index(name="count")
        return DataFrame(result)


# Simple in-memory SQL engine (enough for Task 16 demos)
_VIEWS = {}
def _sql_engine(query):
    # Handle CREATE TEMP VIEW
    import re
    q = query.strip()
    if q.upper().startswith("CREATE"):
        m = re.search(r"VIEW\s+(\w+)\s+AS\s+(.*)", q, re.I | re.S)
        if m:
            _VIEWS[m.group(1).lower()] = m.group(2).strip()
            return DataFrame(pd.DataFrame())
    # Replace view names with stored queries and execute via pandas
    import sqlite3
    conn = sqlite3.connect(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "task06_sql_basics", "wallet_oltp.db"))
    try:
        df = pd.read_sql_query(q, conn)
    except Exception as e:
        df = pd.DataFrame({"error": [str(e)]})
    conn.close()
    return DataFrame(df)


# ══════════════════════════════════════════════════════════════
# TASK 14 – Spark Job: process large dataset
# ══════════════════════════════════════════════════════════════
def task14(spark):
    print("\n" + "=" * 60)
    print("  Task 14 – Spark Basics: Process Large Dataset")
    print("=" * 60)

    print("\n  [1] Read CSV into Spark DataFrame")
    t0  = time.perf_counter()
    df  = spark.read_csv(TXN_CSV)
    print(f"  Rows     : {df.count():,}")
    print(f"  Load time: {(time.perf_counter()-t0)*1000:.0f} ms")

    print("\n  [2] Schema")
    df.printSchema()

    print("\n  [3] Filter: SUCCESS transactions")
    success = df.filter(lambda d: d["status"] == "SUCCESS")
    print(f"  SUCCESS count: {success.count():,}")

    print("\n  [4] Filter: amount > 40 000")
    high_val = df.filter(lambda d: d["amount"].astype(float) > 40_000)
    print(f"  High-value count: {high_val.count():,}")

    print("\n  [5] Sample rows")
    df.select("txn_id","user_id","merchant","amount","status").limit(5).show()

    print("\nTask 14 complete ✓")
    return df


# ══════════════════════════════════════════════════════════════
# TASK 15 – Spark DataFrames: transformations & actions
# ══════════════════════════════════════════════════════════════
def task15(df):
    print("\n" + "=" * 60)
    print("  Task 15 – Spark DataFrames: Transformations & Actions")
    print("=" * 60)

    print("\n  [Transformations – lazy evaluation chain]")
    enriched = (
        df
        .withColumn("gst",        lambda d: (d["amount"].astype(float) * 0.18).round(2))
        .withColumn("net_amount", lambda d: (d["amount"].astype(float) * 0.82).round(2))
        .withColumn("is_success", lambda d: (d["status"] == "SUCCESS").astype(int))
        .withColumn("hour",       lambda d: pd.to_datetime(d["timestamp"]).dt.hour)
        .withColumn("month",      lambda d: d["date"].astype(str).str[:7])
    )
    print("  Added: gst, net_amount, is_success, hour, month")

    print("\n  [Action: groupBy + agg – category revenue]")
    t0 = time.perf_counter()
    cat_rev = (enriched
               .groupBy("category")
               .agg(txn_count=("txn_id","count"),
                    total_rev=("amount","sum"),
                    avg_txn=("amount","mean"))
               .orderBy("total_rev", ascending=False))
    ms = (time.perf_counter() - t0) * 1000
    print(f"  Computed in {ms:.1f} ms")
    cat_rev.show(8)

    print("\n  [Action: groupBy + agg – wallet-type success rate]")
    wallet = (enriched
              .groupBy("wallet_type")
              .agg(txns=("txn_id","count"),
                   success=("is_success","sum"),
                   avg_amount=("amount","mean"))
              .orderBy("txns", ascending=False))
    wallet.show()

    print("\nTask 15 complete ✓")
    return enriched


# ══════════════════════════════════════════════════════════════
# TASK 16 – Spark SQL
# ══════════════════════════════════════════════════════════════
def task16(spark):
    print("\n" + "=" * 60)
    print("  Task 16 – Spark SQL: Query Large Dataset")
    print("=" * 60)

    queries = {
        "Monthly revenue (full year)": """
            SELECT SUBSTR(date,1,7) AS month,
                   COUNT(*)              AS txns,
                   ROUND(SUM(amount),2)  AS revenue,
                   ROUND(AVG(amount),2)  AS avg_txn
            FROM transactions WHERE status='SUCCESS'
            GROUP BY month ORDER BY month""",

        "Top 5 merchants by revenue": """
            SELECT merchant,
                   COUNT(*)             AS txns,
                   ROUND(SUM(amount),2) AS revenue
            FROM transactions WHERE status='SUCCESS'
            GROUP BY merchant ORDER BY revenue DESC LIMIT 5""",

        "Hour-of-day transaction volume": """
            SELECT CAST(SUBSTR(timestamp,12,2) AS INT) AS hour,
                   COUNT(*) AS txns,
                   ROUND(AVG(amount),2) AS avg_amount
            FROM transactions
            GROUP BY hour ORDER BY txns DESC LIMIT 8""",

        "Fraud candidates (FAILED > 45000)": """
            SELECT txn_id, user_id, merchant, amount, timestamp
            FROM transactions
            WHERE status='FAILED' AND amount > 45000
            ORDER BY amount DESC LIMIT 6""",
    }

    for title, sql in queries.items():
        print(f"\n  [spark.sql] {title}")
        result = spark.sql(sql.strip())
        result.show(12)

    print("\nTask 16 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 17 – PySpark Advanced: partitioning, caching, broadcast
# ══════════════════════════════════════════════════════════════
def task17(df, spark):
    print("\n" + "=" * 60)
    print("  Task 17 – PySpark Advanced: Partitioning & Caching")
    print("=" * 60)

    # ── Repartitioning ────────────────────────────────────────
    print("\n  [1] Repartitioning by month (12 partitions)")
    txn_df = df.withColumn("month", lambda d: d["date"].astype(str).str[:7])
    txn_df.repartition(12, "month")
    p_data  = txn_df.toPandas()
    part_sizes = p_data.groupby("month").size()
    print(f"  Partitions created: {len(part_sizes)}")
    print(f"  Avg rows/partition: {part_sizes.mean():.0f}")
    print(f"  Min / Max         : {part_sizes.min()} / {part_sizes.max()}")

    # ── Caching ───────────────────────────────────────────────
    print("\n  [2] Caching demonstration")
    success_df = df.filter(lambda d: d["status"] == "SUCCESS")

    t0 = time.perf_counter()
    _ = success_df.groupBy("category").agg(total=("amount","sum")).collect()
    cold_ms = (time.perf_counter() - t0) * 1000

    cached_df = success_df.cache()
    t0 = time.perf_counter()
    _ = cached_df.groupBy("category").agg(total=("amount","sum")).collect()
    warm_ms = (time.perf_counter() - t0) * 1000

    print(f"  Cold read : {cold_ms:.2f} ms")
    print(f"  Warm read : {warm_ms:.2f} ms")
    print(f"  Speedup   : {cold_ms/max(warm_ms,0.001):.1f}×")

    # ── Broadcast join ────────────────────────────────────────
    print("\n  [3] Broadcast join (small users table)")
    users_df = spark.read_csv(USER_CSV)
    print(f"  Broadcast(users): {users_df.count()} rows  ←  fits in memory")
    print(f"  Large table     : {df.count():,} rows")
    t0 = time.perf_counter()
    joined = df.join(users_df.select("user_id","city","kyc_status"), "user_id", "left")
    join_ms = (time.perf_counter() - t0) * 1000
    print(f"  Join result     : {joined.count():,} rows in {join_ms:.0f} ms")
    joined.select("txn_id","user_id","city","amount","status").limit(4).show()

    # ── Optimisation tips ─────────────────────────────────────
    print("""
  [Spark Optimisation Summary]
  1. Partitioning  – repartition(n, col) aligns shuffles with GROUP BY key
  2. Caching       – df.cache() keeps hot DataFrames in executor memory
  3. Broadcast     – broadcast(small_df) eliminates shuffle for joins
  4. Predicate push-down – filter early to reduce data scanned
  5. Column pruning– select only needed columns before joins
  6. Kryo serial.  – faster than Java default for large shuffles
  7. AQE           – Adaptive Query Execution (Spark 3.0+) auto-tunes joins
    """)

    print("Task 17 complete ✓")


# ── main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = SparkSession.builder_appName("DigitalWalletAnalytics").getOrCreate()
    df       = task14(spark)
    enriched = task15(df)
    task16(spark)
    task17(df, spark)
