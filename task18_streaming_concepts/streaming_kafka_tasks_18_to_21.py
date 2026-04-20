"""
Tasks 18–21 – Streaming & Kafka
  18: Streaming concepts – batch vs stream, use cases
  19: Kafka producer-consumer pipeline
  20: Kafka partitioning and offset management
  21: Structured Streaming – real-time log processing
"""
import csv, json, os, time, random
from collections import defaultdict, deque
from datetime import datetime

BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXN_CSV  = os.path.join(BASE, "data", "transactions.csv")

with open(TXN_CSV) as f:
    ALL_TRANSACTIONS = list(csv.DictReader(f))

random.seed(42)


# ══════════════════════════════════════════════════════════════
# TASK 18 – Streaming Concepts
# ══════════════════════════════════════════════════════════════
def task18():
    print("=" * 60)
    print("  Task 18 – Streaming Concepts")
    print("=" * 60)
    print("""
  ┌──────────────────────────────────────────────────────────┐
  │  BATCH PROCESSING              STREAM PROCESSING         │
  ├──────────────────────────────────────────────────────────┤
  │  Collects data over period     Processes as data arrives │
  │  High latency (hrs/days)       Low latency (ms/sec)      │
  │  Large chunks                  Event-by-event or micro   │
  │  Example: daily settlement     Example: fraud detection  │
  │  Tools: Spark batch, Hadoop    Tools: Kafka, Flink, SS   │
  └──────────────────────────────────────────────────────────┘

  [Digital Wallet Streaming Use Cases]
  • Real-time balance update after every payment
  • Instant fraud alert when transaction looks anomalous
  • Live merchant dashboard refreshing every second
  • Streaming anomaly detection on transaction velocity
  • Push notification triggered on transaction completion

  [Streaming Architecture for Wallet System]

  Mobile App
      │
      ▼  HTTPS POST
  API Gateway
      │
      ▼  produce
  Kafka Topic: wallet-transactions  (4 partitions)
      │
      ├─▶  Fraud Detection Service  (Kafka Streams)
      │         └─▶ Fraud Alerts Topic
      │
      ├─▶  Balance Update Service   (Kafka Consumer)
      │         └─▶ PostgreSQL OLTP DB
      │
      └─▶  Analytics Service        (Spark Structured Streaming)
                └─▶ Data Warehouse (S3 + BigQuery)
    """)
    print("Task 18 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 19 – Kafka Producer-Consumer
# ══════════════════════════════════════════════════════════════
class KafkaTopic:
    """In-memory Kafka topic with partitioned queues."""
    def __init__(self, name: str, num_partitions: int = 4):
        self.name           = name
        self.num_partitions = num_partitions
        self.partitions     = [deque() for _ in range(num_partitions)]
        self.offsets        = [0] * num_partitions         # high-water marks
        self.consumer_offsets: dict = {}                   # group → [offset × partition]

    def partition_for(self, key: str) -> int:
        """Consistent hashing: same user_id always goes to same partition."""
        return hash(key) % self.num_partitions


class KafkaProducer:
    def __init__(self, topic: KafkaTopic):
        self.topic     = topic
        self.sent      = 0
        self.bytes_out = 0

    def send(self, key: str, value: dict) -> tuple:
        p       = self.topic.partition_for(key)
        offset  = self.topic.offsets[p]
        payload = json.dumps(value).encode()
        self.topic.partitions[p].append({
            "key":       key,
            "value":     value,
            "offset":    offset,
            "partition": p,
            "ts":        datetime.now().isoformat(),
            "size":      len(payload),
        })
        self.topic.offsets[p] += 1
        self.sent      += 1
        self.bytes_out += len(payload)
        return p, offset


class KafkaConsumer:
    def __init__(self, topic: KafkaTopic, group_id: str, partitions: list):
        self.topic      = topic
        self.group_id   = group_id
        self.partitions = partitions
        if group_id not in topic.consumer_offsets:
            topic.consumer_offsets[group_id] = [0] * topic.num_partitions
        self.records_consumed = 0

    def poll(self, max_per_partition: int = 10) -> list:
        records = []
        for p in self.partitions:
            committed = self.topic.consumer_offsets[self.group_id][p]
            batch = [m for m in list(self.topic.partitions[p])
                     if m["offset"] >= committed][:max_per_partition]
            if batch:
                self.topic.consumer_offsets[self.group_id][p] = batch[-1]["offset"] + 1
                records.extend(batch)
        self.records_consumed += len(records)
        return records

    def committed_offsets(self):
        return self.topic.consumer_offsets[self.group_id]


def task19():
    print("\n" + "=" * 60)
    print("  Task 19 – Kafka Basics: Producer-Consumer Pipeline")
    print("=" * 60)

    topic    = KafkaTopic("wallet-transactions", num_partitions=4)
    producer = KafkaProducer(topic)

    # Produce 50 messages
    sample = random.sample(ALL_TRANSACTIONS, 50)
    print(f"\n  [Producer] Sending {len(sample)} transaction messages...")
    for txn in sample:
        producer.send(txn["user_id"], txn)

    print(f"  Messages sent     : {producer.sent}")
    print(f"  Bytes produced    : {producer.bytes_out:,}")
    print(f"  Partition offsets : {topic.offsets}")

    # Two consumer groups
    fraud_consumer     = KafkaConsumer(topic, "fraud-detection-group",   [0, 1, 2, 3])
    analytics_consumer = KafkaConsumer(topic, "analytics-group",         [0, 1, 2, 3])

    print(f"\n  [Consumer: fraud-detection-group] Polling messages...")
    fraud_records = fraud_consumer.poll(max_per_partition=5)
    print(f"  Consumed : {len(fraud_records)} records")
    for r in fraud_records[:4]:
        v = r["value"]
        print(f"  P{r['partition']} O{r['offset']:>3} | {v['txn_id']} | "
              f"INR {float(v['amount']):>10,.2f} | {v['status']}")

    print(f"\n  [Consumer: analytics-group] Polling messages (independent offset)...")
    analytics_records = analytics_consumer.poll(max_per_partition=3)
    print(f"  Consumed : {len(analytics_records)} records")

    print(f"\n  [Committed Offsets]")
    print(f"  fraud-detection-group : {fraud_consumer.committed_offsets()}")
    print(f"  analytics-group       : {analytics_consumer.committed_offsets()}")

    print("\nTask 19 complete ✓")
    return topic


# ══════════════════════════════════════════════════════════════
# TASK 20 – Kafka Advanced: partitioning + offset management
# ══════════════════════════════════════════════════════════════
def task20(topic: KafkaTopic):
    print("\n" + "=" * 60)
    print("  Task 20 – Kafka Advanced: Partitioning & Offset Mgmt")
    print("=" * 60)

    print(f"""
  [Why Partitioning?]
  • Parallelism  – each consumer in a group handles one partition
  • Ordering     – messages with the same key land in same partition
  • Throughput   – distribute load across brokers
  • Retention    – per-partition offset tracking enables replay

  [Partition Assignment in this Wallet System]
  Key = user_id  →  hash(user_id) % num_partitions
  • Same user's transactions always go to the same partition
  • Guarantees per-user ordering (critical for balance accuracy)
    """)

    print("  [Current partition distribution]")
    for i, pq in enumerate(topic.partitions):
        print(f"  Partition {i}: {len(pq):>4} messages | "
              f"high-water-mark offset = {topic.offsets[i]}")

    print(f"\n  [Offset Management – seek & replay]")
    # Simulate a consumer seeking back to replay from offset 0
    group = "audit-replay-group"
    topic.consumer_offsets[group] = [0] * topic.num_partitions
    replay_consumer = KafkaConsumer(topic, group, [0])
    replayed = replay_consumer.poll(max_per_partition=3)
    print(f"  Replayed {len(replayed)} messages from partition-0 offset 0")
    for r in replayed[:2]:
        print(f"  Replayed: {r['value']['txn_id']} | offset={r['offset']}")

    print(f"""
  [Offset Strategies]
  earliest   – start from the first message (replay all)
  latest     – start from next new message (skip history)
  committed  – resume from last committed offset (normal use)
  specific   – seek(partition, offset) for targeted replay

  [Delivery Semantics]
  At-most-once  – fire and forget (possible data loss)
  At-least-once – commit after process (possible duplicates)
  Exactly-once  – idempotent producer + transactional consumer
  → Wallet system uses EXACTLY-ONCE for payment processing
    """)

    print("Task 20 complete ✓")


# ══════════════════════════════════════════════════════════════
# TASK 21 – Structured Streaming
# ══════════════════════════════════════════════════════════════
class StructuredStreamingJob:
    """Simulates Spark Structured Streaming with micro-batch processing."""

    def __init__(self, watermark_seconds: int = 30, trigger_interval: float = 1.0):
        self.watermark_s      = watermark_seconds
        self.trigger_interval = trigger_interval
        self.window_totals    = defaultdict(float)
        self.window_counts    = defaultdict(int)
        self.fraud_alerts     = []
        self.batch_stats      = []

    def process_micro_batch(self, batch: list, batch_id: int) -> dict:
        t0       = time.perf_counter()
        cat_rev  = defaultdict(float)
        status_c = defaultdict(int)
        alerts   = []

        for txn in batch:
            amt    = float(txn["amount"])
            cat    = txn["category"]
            status = txn["status"]
            cat_rev[cat]    += amt
            status_c[status] += 1
            self.window_totals[cat] += amt
            self.window_counts[cat] += 1

            # Fraud rule: FAILED transaction with amount > 48 000
            if status == "FAILED" and amt > 48_000:
                alerts.append({"txn_id": txn["txn_id"],
                                "amount": amt, "ts": txn["timestamp"]})

        self.fraud_alerts.extend(alerts)
        ms = (time.perf_counter() - t0) * 1000
        stat = {"batch_id": batch_id, "records": len(batch),
                "latency_ms": round(ms, 2), "alerts": len(alerts),
                "by_category": dict(cat_rev), "by_status": dict(status_c)}
        self.batch_stats.append(stat)
        return stat

    def run(self, source: list, batch_size: int = 1000, num_batches: int = 6):
        print(f"\n  [Streaming config]")
        print(f"  Trigger interval : {self.trigger_interval}s  (micro-batch)")
        print(f"  Watermark        : {self.watermark_s}s  (late-data tolerance)")
        print(f"  Batch size       : {batch_size:,} records")
        print(f"  Output mode      : Complete (running window aggregation)")

        print(f"\n  [Processing micro-batches...]")
        print(f"  {'Batch':>6} {'Records':>8} {'Latency':>9} {'Fraud Alerts':>13}")
        print(f"  {'─'*42}")

        for i in range(num_batches):
            batch = source[i * batch_size: (i + 1) * batch_size]
            stat  = self.process_micro_batch(batch, i)
            print(f"  {i:>6} {stat['records']:>8,} {stat['latency_ms']:>8.2f}ms "
                  f"{stat['alerts']:>13}")

        print(f"\n  [Windowed aggregation – running totals after {num_batches} batches]")
        print(f"  {'Category':<15} {'Total INR':>16} {'Count':>8} {'Avg INR':>12}")
        print(f"  {'─'*54}")
        for cat in sorted(self.window_totals, key=self.window_totals.get, reverse=True):
            total = self.window_totals[cat]
            count = self.window_counts[cat]
            print(f"  {cat:<15} {total:>16,.2f} {count:>8,} {total/count:>12,.2f}")

        if self.fraud_alerts:
            print(f"\n  [Fraud Alerts triggered: {len(self.fraud_alerts)}]")
            for alert in self.fraud_alerts[:3]:
                print(f"  ALERT: {alert['txn_id']} | INR {alert['amount']:,.2f} | {alert['ts']}")


def task21():
    print("\n" + "=" * 60)
    print("  Task 21 – Structured Streaming: Real-Time Log Processing")
    print("=" * 60)

    job = StructuredStreamingJob(watermark_seconds=30, trigger_interval=1.0)
    job.run(ALL_TRANSACTIONS, batch_size=1000, num_batches=6)

    print(f"""
  [Spark Structured Streaming – Key Concepts]

  readStream    – reads from Kafka / file / socket as a stream
  writeStream   – outputs to console / Parquet / Kafka / DB

  Output modes:
    Append   – only new rows (good for logs)
    Complete – full updated result (good for aggregations)
    Update   – only changed rows

  Watermark:
    .withWatermark("event_time", "30 seconds")
    Tells Spark how long to wait for late-arriving data
    before finalising a window result.

  Example PySpark code:
    df = spark.readStream \\
         .format("kafka") \\
         .option("kafka.bootstrap.servers","broker:9092") \\
         .option("subscribe","wallet-transactions") \\
         .load()

    query = df.groupBy(
              window("timestamp","5 minutes"),
              "category") \\
           .agg(sum("amount").alias("revenue")) \\
           .writeStream \\
           .outputMode("complete") \\
           .format("console") \\
           .trigger(processingTime="10 seconds") \\
           .start()
    """)
    print("Task 21 complete ✓")


# ── main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    task18()
    topic = task19()
    task20(topic)
    task21()
