"""
Task 05 – Pandas + NumPy
Large-scale data analysis (1 M+ rows), memory optimisation,
vectorised operations, and performance benchmarks.
"""
import os, time
import pandas as pd
import numpy as np

BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_CSV = os.path.join(BASE, "data", "transactions.csv")


def mb(df: pd.DataFrame) -> float:
    return df.memory_usage(deep=True).sum() / 1024 ** 2


# ── 1. Load base + expand to 1 M+ rows ───────────────────────────────────────
print("=" * 60)
print("  Task 05 – Pandas + NumPy: Large-Scale Analysis")
print("=" * 60)

print("\n[1] Loading base dataset (100 000 rows)...")
t0     = time.perf_counter()
df_base = pd.read_csv(DATA_CSV, parse_dates=["timestamp", "date"])
print(f"  Rows     : {len(df_base):,}")
print(f"  Memory   : {mb(df_base):.2f} MB")
print(f"  Load time: {(time.perf_counter()-t0)*1000:.1f} ms")

print("\n[2] Replicating to 1 M+ rows...")
df = pd.concat([df_base] * 11, ignore_index=True)
df["txn_id"] = [f"TXN{str(i+1).zfill(8)}" for i in range(len(df))]
rng  = np.random.default_rng(42)
noise = rng.normal(0, 50, len(df))
df["amount"] = (df["amount"].astype("float64") + noise).clip(lower=1.0).round(2)
print(f"  Rows     : {len(df):,}")
print(f"  Memory   : {mb(df):.2f} MB")

# ── 2. Memory optimisation ────────────────────────────────────────────────────
print("\n[3] Memory Optimisation (category dtype + float32)...")
mem_before = mb(df)
for col in ["status", "category", "merchant", "wallet_type"]:
    df[col] = df[col].astype("category")
df["amount"] = df["amount"].astype("float32")
mem_after = mb(df)
print(f"  Before  : {mem_before:.2f} MB")
print(f"  After   : {mem_after:.2f} MB")
print(f"  Saved   : {mem_before - mem_after:.2f} MB ({(1-mem_after/mem_before)*100:.1f}% reduction)")

# ── 3. NumPy statistical analysis ────────────────────────────────────────────
print("\n[4] NumPy Statistical Analysis...")
amounts = df["amount"].to_numpy(dtype=np.float64)
t0 = time.perf_counter()
stats = {
    "count" : len(amounts),
    "sum"   : np.sum(amounts),
    "mean"  : np.mean(amounts),
    "median": np.median(amounts),
    "std"   : np.std(amounts),
    "min"   : np.min(amounts),
    "max"   : np.max(amounts),
    "p25"   : float(np.percentile(amounts, 25)),
    "p75"   : float(np.percentile(amounts, 75)),
    "p95"   : float(np.percentile(amounts, 95)),
}
t1 = time.perf_counter()
print(f"  Computed in {(t1-t0)*1000:.2f} ms")
for k, v in stats.items():
    if k == "count":
        print(f"  {k:<8}: {int(v):>14,}")
    elif k == "sum":
        print(f"  {k:<8}: INR {v:>14,.0f}")
    else:
        print(f"  {k:<8}: {v:>14.4f}")

# ── 4. GroupBy analytics ──────────────────────────────────────────────────────
print("\n[5] GroupBy: Category Revenue...")
t0  = time.perf_counter()
grp = (df.groupby("category", observed=True)["amount"]
         .agg(["count", "sum", "mean", "std"])
         .sort_values("sum", ascending=False))
print(f"  Computed in {(time.perf_counter()-t0)*1000:.1f} ms")
print(grp.round(2).to_string())

# ── 5. Vectorised vs loop benchmark ──────────────────────────────────────────
print("\n[6] Performance – Loop vs Vectorised (100 000 ops)...")
sample = amounts[:100_000]

t0 = time.perf_counter()
_ = sum(v * 1.18 for v in sample)
loop_ms = (time.perf_counter() - t0) * 1000

t0 = time.perf_counter()
_ = np.sum(sample * 1.18)
vec_ms = (time.perf_counter() - t0) * 1000

print(f"  Python loop  : {loop_ms:.2f} ms")
print(f"  NumPy vector : {vec_ms:.2f} ms")
print(f"  Speedup      : {loop_ms / max(vec_ms, 0.001):.0f}x")

# ── 6. Feature engineering ────────────────────────────────────────────────────
print("\n[7] Feature Engineering...")
df["hour"]       = df["timestamp"].dt.hour
df["day_of_week"]= df["timestamp"].dt.dayofweek
df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype("int8")
df["log_amount"] = np.log1p(df["amount"])
print(f"  Added: hour, day_of_week, is_weekend, log_amount")

corr = df[["amount", "hour", "day_of_week", "is_weekend"]].corr()
print("\n  Correlation with 'amount':")
print(corr["amount"].drop("amount").round(4).to_string())

print("\nTask 05 complete ✓")
