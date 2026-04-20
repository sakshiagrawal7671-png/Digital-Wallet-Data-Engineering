"""
Task 04 – Advanced Python
Reusable transformation modules:
  • Normalizer  – min-max, z-score, log transform, string normalise
  • Aggregator  – group-by sum/count/avg, percentile, rolling window
  • Validator   – schema, range, enum, format checks, batch validation
"""
import csv, math, re, os
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ══════════════════════════════════════════════════════════════
# MODULE 1 – Normalizer
# ══════════════════════════════════════════════════════════════
class Normalizer:
    """Numerical and text normalisation helpers."""

    @staticmethod
    def min_max(values: list[float]) -> list[float]:
        """Scale values to [0, 1]."""
        lo, hi = min(values), max(values)
        if hi == lo:
            return [0.0] * len(values)
        return [(v - lo) / (hi - lo) for v in values]

    @staticmethod
    def z_score(values: list[float]) -> list[float]:
        """Standardise to mean=0, std=1."""
        n    = len(values)
        mean = sum(values) / n
        std  = math.sqrt(sum((v - mean) ** 2 for v in values) / n)
        if std == 0:
            return [0.0] * n
        return [(v - mean) / std for v in values]

    @staticmethod
    def log_transform(values: list[float]) -> list[float]:
        """log(1+x) transform – useful for right-skewed amounts."""
        return [math.log1p(max(0.0, v)) for v in values]

    @staticmethod
    def clean_string(s: str) -> str:
        """Strip, lower, collapse whitespace, remove special chars."""
        s = s.strip().lower()
        s = re.sub(r"[^\w\s]", "", s)
        return re.sub(r"\s+", "_", s)

    @staticmethod
    def parse_amount(raw: str, currency: str = "INR") -> dict:
        """Extract numeric value from messy amount strings."""
        cleaned = re.sub(r"[^\d.]", "", str(raw))
        try:
            val = round(float(cleaned), 2)
            return {"value": val, "currency": currency, "valid": True}
        except ValueError:
            return {"value": 0.0, "currency": currency, "valid": False}


# ══════════════════════════════════════════════════════════════
# MODULE 2 – Aggregator
# ══════════════════════════════════════════════════════════════
class Aggregator:
    """Group-by and statistical aggregation utilities."""

    @staticmethod
    def group_by(rows: list[dict], key: str) -> dict:
        groups: dict = {}
        for row in rows:
            groups.setdefault(row.get(key, "UNKNOWN"), []).append(row)
        return groups

    @staticmethod
    def sum_by(rows: list[dict], group_key: str, value_key: str) -> dict:
        totals: dict = {}
        for row in rows:
            try:
                totals[row[group_key]] = totals.get(row[group_key], 0.0) + float(row[value_key])
            except (ValueError, KeyError):
                pass
        return {k: round(v, 2) for k, v in totals.items()}

    @staticmethod
    def count_by(rows: list[dict], group_key: str) -> dict:
        counts: dict = {}
        for row in rows:
            k = row.get(group_key, "UNKNOWN")
            counts[k] = counts.get(k, 0) + 1
        return counts

    @staticmethod
    def avg_by(rows: list[dict], group_key: str, value_key: str) -> dict:
        sums: dict   = {}
        cnts: dict   = {}
        for row in rows:
            try:
                k = row[group_key]
                sums[k] = sums.get(k, 0.0) + float(row[value_key])
                cnts[k] = cnts.get(k, 0)   + 1
            except (ValueError, KeyError):
                pass
        return {k: round(sums[k] / cnts[k], 2) for k in sums if cnts[k]}

    @staticmethod
    def percentile(values: list[float], p: float) -> float:
        """p-th percentile (0–100)."""
        sv  = sorted(values)
        idx = (p / 100) * (len(sv) - 1)
        lo  = int(idx)
        return round(sv[lo] + (idx - lo) * (sv[lo + 1] - sv[lo])
                     if lo + 1 < len(sv) else sv[lo], 2)

    @staticmethod
    def rolling_sum(values: list[float], window: int) -> list[float]:
        return [sum(values[max(0, i - window + 1): i + 1]) for i in range(len(values))]


# ══════════════════════════════════════════════════════════════
# MODULE 3 – Validator
# ══════════════════════════════════════════════════════════════
class Validator:
    """Row-level and batch validation for wallet transactions."""

    TXN_RE   = re.compile(r"^TXN\d{8}$")
    USER_RE  = re.compile(r"^U\d{4}$")
    DATE_FMT = "%Y-%m-%d %H:%M:%S"

    VALID_STATUSES = {"SUCCESS", "FAILED", "PENDING", "REFUNDED"}
    VALID_CATS     = {"Shopping","Food","Transport","Entertainment",
                      "Utilities","Travel","Grocery","Transfer"}
    MIN_AMT, MAX_AMT = 1.0, 1_000_000.0

    @classmethod
    def validate_row(cls, row: dict) -> dict:
        errors = []

        if not cls.TXN_RE.match(row.get("txn_id", "")):
            errors.append("txn_id must match TXNxxxxxxxx")
        if not cls.USER_RE.match(row.get("user_id", "")):
            errors.append("user_id must match Uxxxx")

        try:
            amt = float(row.get("amount", -1))
            if not (cls.MIN_AMT <= amt <= cls.MAX_AMT):
                errors.append(f"amount {amt} outside [{cls.MIN_AMT}, {cls.MAX_AMT}]")
        except ValueError:
            errors.append("amount is not numeric")

        if row.get("status", "") not in cls.VALID_STATUSES:
            errors.append(f"unknown status: {row.get('status')}")

        if row.get("category", "") not in cls.VALID_CATS:
            errors.append(f"unknown category: {row.get('category')}")

        try:
            datetime.strptime(row.get("timestamp", ""), cls.DATE_FMT)
        except ValueError:
            errors.append("timestamp must be YYYY-MM-DD HH:MM:SS")

        return {"valid": not errors, "errors": errors}

    @classmethod
    def validate_batch(cls, rows: list[dict]) -> dict:
        valid, invalid = [], []
        for row in rows:
            result = cls.validate_row(row)
            (valid if result["valid"] else invalid).append(row)
        return {
            "total":       len(rows),
            "valid":       len(valid),
            "invalid":     len(invalid),
            "valid_pct":   round(len(valid) / len(rows) * 100, 2) if rows else 0,
            "valid_rows":  valid,
            "invalid_rows": invalid,
        }


# ──────────────────────────────────────────────────────────────
# Demo / main
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  Task 04 – Advanced Python: Reusable Modules")
    print("=" * 55)

    # Load data
    rows = []
    with open(os.path.join(BASE, "data", "transactions.csv")) as f:
        rows = list(csv.DictReader(f))
    amounts = [float(r["amount"]) for r in rows[:500]]

    # ── Normalizer ──────────────────────────────────────────
    norm = Normalizer()
    mm   = norm.min_max(amounts[:8])
    zs   = norm.z_score(amounts[:8])
    lg   = norm.log_transform(amounts[:8])

    print("\n[Normalizer]")
    print(f"  Raw (first 5)     : {[round(a,2) for a in amounts[:5]]}")
    print(f"  Min-Max (first 5) : {[round(v,4) for v in mm[:5]]}")
    print(f"  Z-Score (first 5) : {[round(v,4) for v in zs[:5]]}")
    print(f"  Log1p   (first 5) : {[round(v,4) for v in lg[:5]]}")
    print(f"  clean_string      : '{norm.clean_string('  PhonePe Payment!  ')}'")
    print(f"  parse_amount      : {norm.parse_amount('Rs. 1,299.50')}")

    # ── Aggregator ──────────────────────────────────────────
    agg   = Aggregator()
    s_by  = agg.sum_by(rows,   "category", "amount")
    c_by  = agg.count_by(rows, "category")
    a_by  = agg.avg_by(rows,   "category", "amount")

    print("\n[Aggregator]  Top 4 categories by revenue:")
    print(f"  {'Category':<15} {'Count':>6} {'Total (INR)':>16} {'Avg (INR)':>12}")
    print(f"  {'─'*52}")
    for cat in sorted(s_by, key=s_by.get, reverse=True)[:4]:
        print(f"  {cat:<15} {c_by[cat]:>6,} {s_by[cat]:>16,.2f} {a_by[cat]:>12,.2f}")

    p25 = agg.percentile(amounts, 25)
    p75 = agg.percentile(amounts, 75)
    p95 = agg.percentile(amounts, 95)
    print(f"\n  Percentiles P25/P75/P95: {p25:,.2f} / {p75:,.2f} / {p95:,.2f}")

    # ── Validator ───────────────────────────────────────────
    val    = Validator()
    result = val.validate_batch(rows[:200])

    print(f"\n[Validator]  Batch of 200 rows:")
    print(f"  Total  : {result['total']}")
    print(f"  Valid  : {result['valid']}  ({result['valid_pct']}%)")
    print(f"  Invalid: {result['invalid']}")

    bad = {"txn_id":"BAD1","user_id":"XX","amount":"-50",
           "status":"DONE","category":"Gambling","timestamp":"not-a-date"}
    print(f"\n  Bad row errors: {val.validate_row(bad)['errors']}")

    print("\nTask 04 complete ✓")
