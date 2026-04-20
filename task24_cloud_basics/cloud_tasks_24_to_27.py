"""
Tasks 24–27 – Cloud Services
  24: Compare AWS, GCP, Azure services
  25: Simulate S3/GCS upload and retrieval
  26: Simulate EC2 instance + data pipeline deployment
  27: Simulate BigQuery/Redshift analytics on large data
"""
import csv, json, os, sqlite3, time, hashlib
from datetime import datetime

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXN_CSV = os.path.join(BASE, "data", "transactions.csv")
OLTP_DB = os.path.join(BASE, "task06_sql_basics", "wallet_oltp.db")


# ══════════════════════════════════════════════════════════════
# TASK 24 – Cloud Provider Comparison
# ══════════════════════════════════════════════════════════════
def task24():
    print("=" * 65)
    print("  Task 24 – Cloud Basics: AWS vs GCP vs Azure")
    print("=" * 65)

    services = [
        ("Compute",         "EC2",               "Compute Engine",    "Azure VMs"),
        ("Object Storage",  "S3",                "Cloud Storage",     "Blob Storage"),
        ("Managed SQL",     "RDS",               "Cloud SQL",         "Azure SQL DB"),
        ("NoSQL",           "DynamoDB",          "Firestore",         "Cosmos DB"),
        ("Data Warehouse",  "Redshift",          "BigQuery",          "Synapse Analytics"),
        ("Stream",          "Kinesis",           "Pub/Sub",           "Event Hubs"),
        ("Serverless",      "Lambda",            "Cloud Functions",   "Azure Functions"),
        ("ETL/Pipelines",   "AWS Glue",          "Dataflow",          "Data Factory"),
        ("Orchestration",   "MWAA (Airflow)",    "Cloud Composer",    "Azure Data Factory"),
        ("ML Platform",     "SageMaker",         "Vertex AI",         "Azure ML"),
        ("Lakehouse",       "S3 + Lake Formation","GCS + BigLake",    "ADLS + Synapse"),
        ("CDN",             "CloudFront",        "Cloud CDN",         "Azure CDN"),
        ("Pricing Model",   "Pay-per-use",       "Per-second billing","Reserved + PAYG"),
    ]

    print(f"\n  {'Service':<18} {'AWS':<22} {'GCP':<22} {'Azure'}")
    print("  " + "─" * 85)
    for row in services:
        print(f"  {row[0]:<18} {row[1]:<22} {row[2]:<22} {row[3]}")

    print("""
  [Recommended Cloud Stack for Digital Wallet System]
  Layer             Service                     Provider
  ───────────────────────────────────────────────────────
  OLTP DB           RDS PostgreSQL              AWS
  Data Lake (raw)   S3 (Bronze layer)           AWS
  Data Lake (clean) GCS (Silver/Gold)           GCP
  Data Warehouse    BigQuery (serverless)       GCP
  Streaming         Kinesis / Kafka MSK         AWS
  Orchestration     MWAA (Managed Airflow)      AWS
  ML / Fraud        SageMaker                   AWS
  Dashboard         Looker / QuickSight         GCP/AWS

  [Why BigQuery for DW?]
  • Serverless – no cluster to manage
  • Columnar storage – fast on aggregations
  • Charged per byte scanned, not uptime
  • Native Pandas connector (pandas-gbq)
    """)
    print("Task 24 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 25 – Cloud Storage (S3 / GCS simulation)
# ══════════════════════════════════════════════════════════════
class CloudBucket:
    """Simulates an S3 / GCS bucket using local filesystem."""
    def __init__(self, name: str, provider: str = "AWS S3"):
        self.name     = name
        self.provider = provider
        self._store   = {}           # key → {data, meta}
        self._local   = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "cloud_sim", name)
        os.makedirs(self._local, exist_ok=True)

    def put(self, key: str, data: bytes, metadata: dict | None = None):
        etag  = hashlib.md5(data).hexdigest()
        fpath = os.path.join(self._local, key.replace("/", "_"))
        with open(fpath, "wb") as f:
            f.write(data)
        self._store[key] = {
            "key": key, "size": len(data), "etag": etag,
            "metadata": metadata or {},
            "last_modified": datetime.now().isoformat(),
            "local_path": fpath,
        }
        url = (f"https://{self.name}.s3.amazonaws.com/{key}"
               if "S3" in self.provider
               else f"https://storage.googleapis.com/{self.name}/{key}")
        print(f"  PUT   {key:<60} {len(data):>10,} B  ETag={etag[:8]}")
        return url

    def get(self, key: str) -> bytes:
        obj = self._store.get(key)
        if not obj:
            raise KeyError(f"Object '{key}' not found in bucket '{self.name}'")
        with open(obj["local_path"], "rb") as f:
            data = f.read()
        print(f"  GET   {key:<60} {len(data):>10,} B")
        return data

    def delete(self, key: str):
        if key in self._store:
            os.remove(self._store[key]["local_path"])
            del self._store[key]
            print(f"  DELETE {key}")

    def ls(self, prefix: str = ""):
        matches = {k: v for k, v in self._store.items() if k.startswith(prefix)}
        print(f"\n  ls {self.provider}://{self.name}/{prefix}")
        print(f"  {'Key':<62} {'Size':>10}  Modified")
        print("  " + "─" * 88)
        for k, v in sorted(matches.items()):
            print(f"  {k:<62} {v['size']:>10,}  {v['last_modified'][:19]}")
        print(f"  [{len(matches)} objects]")

    def presigned_url(self, key: str, expiry_s: int = 3600) -> str:
        sig = hashlib.sha256(f"{self.name}{key}{expiry_s}".encode()).hexdigest()[:16]
        url = (f"https://{self.name}.s3.amazonaws.com/{key}"
               f"?X-Amz-Expires={expiry_s}&X-Amz-Signature={sig}")
        print(f"  Presigned URL (expires {expiry_s}s):\n  {url[:90]}…")
        return url


def task25():
    print("\n" + "=" * 65)
    print("  Task 25 – Cloud Storage: S3/GCS Upload & Retrieve")
    print("=" * 65)

    bucket = CloudBucket("wallet-data-lake-prod", "AWS S3")

    print("\n  [Uploading to S3]")
    with open(os.path.join(BASE, "data", "transactions_2023_01.csv"), "rb") as f:
        raw01 = f.read()
    with open(os.path.join(BASE, "data", "users.csv"), "rb") as f:
        users_data = f.read()

    bucket.put("raw/transactions/year=2023/month=01/part-0.csv",   raw01,
               {"source": "etl-pipeline", "format": "csv", "rows": "3000"})
    bucket.put("raw/transactions/year=2023/month=02/part-0.csv",
               b"txn_id,amount,status\nTXN00000001,500,SUCCESS\n",
               {"source": "etl-pipeline", "format": "csv"})
    bucket.put("raw/users/users.csv", users_data,
               {"source": "crm-export", "format": "csv"})
    bucket.put("reports/2023-Q1/monthly_summary.json",
               json.dumps({"month": "2023-01", "total_revenue": 160_940_568.93,
                            "txns": 6377}).encode())

    print("\n  [Listing objects]")
    bucket.ls("raw/")

    print("\n  [Retrieving object]")
    data = bucket.get("raw/users/users.csv")
    rows = list(csv.DictReader(data.decode().splitlines()))
    print(f"  Decoded users: {len(rows):,} rows")

    print("\n  [Presigned URL for secure sharing]")
    bucket.presigned_url("reports/2023-Q1/monthly_summary.json", expiry_s=3600)

    print("""
  [S3 Best Practices for Wallet Data]
  • Versioning        – enabled on all buckets
  • Encryption        – SSE-S3 or SSE-KMS (AES-256)
  • Lifecycle rules   – archive raw/ to Glacier after 90 days
  • Partitioning      – year=/month=/day= for partition pruning
  • Access control    – bucket policies + IAM roles (no public access)
  • Cross-region rep  – CRR to secondary region for DR
    """)
    print("Task 25 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 26 – Cloud Compute: EC2 + Pipeline Deployment
# ══════════════════════════════════════════════════════════════
class EC2Instance:
    def __init__(self, instance_type: str, ami: str):
        self.instance_id   = "i-" + hashlib.md5(f"{instance_type}{ami}".encode()).hexdigest()[:10]
        self.instance_type = instance_type
        self.ami           = ami
        self.state         = "pending"
        self.public_ip     = f"13.{hash(instance_type) % 254}.{hash(ami) % 254}.{hash(self.instance_id) % 254}"
        self.private_ip    = f"10.0.1.{hash(self.instance_id) % 254}"
        self.launched_at   = datetime.now().isoformat()

    def start(self):
        self.state = "running"
        print(f"  Instance     : {self.instance_id}")
        print(f"  Type         : {self.instance_type}")
        print(f"  AMI          : {self.ami}")
        print(f"  Public IP    : {self.public_ip}")
        print(f"  Private IP   : {self.private_ip}")
        print(f"  State        : {self.state}")

    def deploy_pipeline(self):
        steps = [
            ("SSH connect",          f"ssh -i wallet-keypair.pem ubuntu@{self.public_ip}"),
            ("Update packages",      "sudo apt-get update && sudo apt-get upgrade -y"),
            ("Install Python 3.11",  "sudo apt install python3.11 python3-pip -y"),
            ("Install deps",         "pip3 install pandas numpy sqlalchemy psycopg2-binary"),
            ("Clone repo",           "git clone https://github.com/org/wallet-pipeline.git"),
            ("Config .env",          "cp .env.example .env && vim .env"),
            ("Run pipeline",         "python3 wallet-pipeline/task30_final_project/pipeline.py"),
            ("Schedule cron",        "crontab -e  →  0 2 * * * python3 ~/pipeline.py >> cron.log 2>&1"),
            ("Monitor logs",         "tail -f ~/cron.log"),
        ]
        print(f"\n  [Deploying pipeline to {self.instance_id}]")
        for step, cmd in steps:
            print(f"  {step:<22}  $ {cmd}")


def task26():
    print("\n" + "=" * 65)
    print("  Task 26 – Cloud Compute: EC2 Instance & Deployment")
    print("=" * 65)

    print("\n  [Launching EC2 instance via boto3 (simulated)]")
    ec2 = EC2Instance("t3.xlarge", "ami-0c55b159cbfafe1f0")
    ec2.start()
    ec2.deploy_pipeline()

    print(f"""
  [EC2 Sizing Guide for Wallet Pipeline]
  Batch ETL (100K rows/day)  →  t3.medium   (2 vCPU, 4 GB)
  Spark cluster (1 M rows)   →  r5.2xlarge  (8 vCPU, 64 GB)
  Kafka broker               →  m5.xlarge   (4 vCPU, 16 GB)
  Airflow webserver          →  t3.large    (2 vCPU, 8 GB)

  [Auto Scaling]
  aws autoscaling create-auto-scaling-group \\
    --min-size 1 --max-size 10 \\
    --target-tracking-scaling-policy TargetValue=70,PredefinedMetricType=ASGAverageCPUUtilization

  [Cost Optimisation]
  • Spot instances for batch ETL (up to 90% cheaper)
  • Reserved instances for Kafka/Airflow (up to 72% cheaper)
  • Auto-stop non-prod instances after business hours
    """)
    print("Task 26 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 27 – Cloud Data Warehouse (BigQuery / Redshift)
# ══════════════════════════════════════════════════════════════
def task27():
    print("\n" + "=" * 65)
    print("  Task 27 – Cloud DW: BigQuery / Redshift Analytics")
    print("=" * 65)

    print("""
  [BigQuery vs Redshift]
  Feature            BigQuery                  Redshift
  ─────────────────────────────────────────────────────
  Model              Serverless (no cluster)   Cluster-based
  Storage            Columnar (Capacitor fmt)  Columnar (parquet-like)
  Pricing            Per byte scanned          Per node-hour
  Scale              Auto (petabyte)           Manual resize
  SQL                Standard SQL              PostgreSQL-compatible
  Streaming          Native streaming insert   COPY / Kinesis Firehose
  ML                 BigQuery ML (SQL)         SageMaker integration
  Best for           Ad-hoc analytics          Predictable workloads
    """)

    # Simulate BigQuery-style DW queries using our SQLite OLTP DB
    conn = sqlite3.connect(OLTP_DB)

    queries = {
        "Quarterly revenue split": """
            SELECT
              CASE SUBSTR(date,6,2)
                WHEN '01' THEN 'Q1' WHEN '02' THEN 'Q1' WHEN '03' THEN 'Q1'
                WHEN '04' THEN 'Q2' WHEN '05' THEN 'Q2' WHEN '06' THEN 'Q2'
                WHEN '07' THEN 'Q3' WHEN '08' THEN 'Q3' WHEN '09' THEN 'Q3'
                ELSE 'Q4' END AS quarter,
              COUNT(*)              AS txns,
              ROUND(SUM(amount)/1e6,2) AS revenue_M,
              ROUND(AVG(amount),2)  AS avg_txn
            FROM transactions WHERE status='SUCCESS'
            GROUP BY quarter ORDER BY quarter""",

        "City revenue (JOIN with users)": """
            SELECT u.city,
                   COUNT(*)               AS txns,
                   ROUND(SUM(t.amount)/1e6,2) AS revenue_M
            FROM transactions t
            JOIN users u ON t.user_id = u.user_id
            WHERE t.status = 'SUCCESS'
            GROUP BY u.city ORDER BY revenue_M DESC LIMIT 6""",

        "KYC status impact on spend": """
            SELECT u.kyc_status,
                   COUNT(*)               AS txns,
                   ROUND(AVG(t.amount),2) AS avg_amount,
                   ROUND(SUM(t.amount)/1e6,2) AS total_M
            FROM transactions t
            JOIN users u ON t.user_id = u.user_id
            GROUP BY u.kyc_status ORDER BY total_M DESC""",
    }

    def show_query(title, sql):
        print(f"\n  [BigQuery: {title}]")
        t0  = time.perf_counter()
        cur = conn.execute(sql)
        ms  = (time.perf_counter() - t0) * 1000
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        w    = [max(len(str(c)), *(len(str(r[i])) for r in rows))
                for i, c in enumerate(cols)]
        print("  " + " | ".join(str(c).ljust(w[i]) for i, c in enumerate(cols)))
        print("  " + "─" * (sum(w) + 3*len(w)))
        for row in rows:
            print("  " + " | ".join(str(v).ljust(w[i]) for i, v in enumerate(row)))
        print(f"  Query time: {ms:.1f} ms  |  {len(rows)} rows")

    for title, sql in queries.items():
        show_query(title, sql.strip())

    conn.close()
    print("""
  [BigQuery SQL for Wallet DW – example]
  SELECT
    DATE_TRUNC(timestamp, MONTH)  AS month,
    category,
    COUNT(*)                      AS txn_count,
    ROUND(SUM(amount) / 1e6, 2)  AS revenue_M
  FROM `wallet-analytics.transactions.fact`
  WHERE status = 'SUCCESS'
    AND DATE(timestamp) BETWEEN '2023-01-01' AND '2023-12-31'
  GROUP BY 1, 2
  ORDER BY 1, revenue_M DESC
    """)
    print("Task 27 complete ✓")


# ── main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    task24()
    task25()
    task26()
    task27()
