"""
Tasks 12 & 13 – Hadoop / HDFS Architecture
Task 12: Set up HDFS locally (simulated) and upload structured/unstructured data.
Task 13: Explain NameNode, DataNode and simulate data storage + replication.
"""
import os, json, hashlib, csv
from datetime import datetime

BASE      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HDFS_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hdfs_sim")

BLOCK_SIZE  = 64 * 1024   # 64 KB  (real default: 128 MB)
REPLICATION = 3


# ══════════════════════════════════════════════════════════════
# Simulated HDFS cluster
# ══════════════════════════════════════════════════════════════
class NameNode:
    """Manages namespace metadata (file → blocks → datanodes)."""
    def __init__(self, path):
        self.path      = path
        self.namespace = {}          # hdfs_path → file meta
        self.edit_log  = []
        os.makedirs(path, exist_ok=True)

    def register_file(self, hdfs_path, meta):
        self.namespace[hdfs_path] = meta
        self.edit_log.append({
            "op": "CREATE", "path": hdfs_path,
            "ts": datetime.now().isoformat()
        })
        # persist edit log
        with open(os.path.join(self.path, "editlog.json"), "w") as f:
            json.dump(self.edit_log, f, indent=2)

    def get_file(self, hdfs_path):
        return self.namespace.get(hdfs_path)

    def ls(self, prefix="/"):
        return {p: m for p, m in self.namespace.items() if p.startswith(prefix)}


class DataNode:
    """Stores actual data blocks."""
    def __init__(self, node_id, root):
        self.node_id = node_id
        self.root    = os.path.join(root, node_id)
        os.makedirs(self.root, exist_ok=True)
        self.blocks  = {}   # block_id → size

    def write_block(self, block_id, data: bytes):
        path = os.path.join(self.root, f"{block_id}.blk")
        with open(path, "wb") as f:
            f.write(data)
        self.blocks[block_id] = len(data)

    def read_block(self, block_id) -> bytes:
        path = os.path.join(self.root, f"{block_id}.blk")
        with open(path, "rb") as f:
            return f.read()

    def health(self):
        return {"node_id": self.node_id, "status": "ALIVE",
                "blocks": len(self.blocks),
                "bytes_used": sum(self.blocks.values())}


class HDFSCluster:
    """Wraps NameNode + DataNodes into a mini cluster."""
    def __init__(self, root, block_size=BLOCK_SIZE, replication=REPLICATION):
        self.block_size  = block_size
        self.replication = replication
        self.namenode    = NameNode(os.path.join(root, "namenode"))
        self.datanodes   = [DataNode(f"dn_{i}", os.path.join(root, "datanodes"))
                            for i in range(1, 5)]

    # ── put ───────────────────────────────────────────────────
    def put(self, local_path: str, hdfs_path: str):
        with open(local_path, "rb") as f:
            data = f.read()

        chunks = [data[i:i + self.block_size]
                  for i in range(0, len(data), self.block_size)]
        block_metas = []

        for seq, chunk in enumerate(chunks):
            block_id = f"blk_{hashlib.md5(chunk).hexdigest()[:12]}_{seq:04d}"
            replicas = []
            for r in range(self.replication):
                dn = self.datanodes[(seq + r) % len(self.datanodes)]
                dn.write_block(block_id, chunk)
                replicas.append(dn.node_id)
            block_metas.append({"block_id": block_id,
                                 "size":     len(chunk),
                                 "replicas": replicas})

        meta = {
            "hdfs_path":   hdfs_path,
            "local_name":  os.path.basename(local_path),
            "total_bytes": len(data),
            "num_blocks":  len(chunks),
            "replication": self.replication,
            "blocks":      block_metas,
            "uploaded_at": datetime.now().isoformat(),
        }
        self.namenode.register_file(hdfs_path, meta)

        print(f"  put  {os.path.basename(local_path):<40} → {hdfs_path}")
        print(f"       {len(data)/1024:.1f} KB  |  {len(chunks)} block(s)  "
              f"|  {self.replication}× replication")
        return meta

    # ── get ───────────────────────────────────────────────────
    def get(self, hdfs_path: str) -> bytes:
        meta = self.namenode.get_file(hdfs_path)
        if not meta:
            raise FileNotFoundError(hdfs_path)
        result = b""
        for blk in meta["blocks"]:
            dn = next(d for d in self.datanodes if d.node_id == blk["replicas"][0])
            result += dn.read_block(blk["block_id"])
        return result

    # ── ls ────────────────────────────────────────────────────
    def ls(self, prefix="/"):
        files = self.namenode.ls(prefix)
        print(f"\n  hdfs ls {prefix}")
        print(f"  {'Path':<55} {'Size':>10}  {'Blocks':>7}  {'Repl':>5}")
        print("  " + "─" * 82)
        for path, m in files.items():
            print(f"  {path:<55} {m['total_bytes']:>10,}  {m['num_blocks']:>7}  "
                  f"{m['replication']:>5}×")

    # ── fsck ──────────────────────────────────────────────────
    def fsck(self):
        all_blocks = sum(len(m["blocks"]) for m in self.namenode.namespace.values())
        print(f"\n  hdfs fsck /")
        print(f"  Files        : {len(self.namenode.namespace)}")
        print(f"  Total blocks : {all_blocks}")
        print(f"  Expected replicas : {all_blocks * self.replication}")
        print(f"\n  DataNode health:")
        for dn in self.datanodes:
            h = dn.health()
            bar = "█" * (h["blocks"] // max(all_blocks // 20, 1))
            print(f"    {h['node_id']}  {h['status']:<6}  "
                  f"{h['blocks']:>4} blocks  {h['bytes_used']//1024:>6} KB  {bar}")


# ══════════════════════════════════════════════════════════════
# Task 12 main
# ══════════════════════════════════════════════════════════════
def task12():
    print("=" * 60)
    print("  Task 12 – Hadoop: Set Up HDFS & Upload Data")
    print("=" * 60)

    cluster = HDFSCluster(HDFS_ROOT)

    # Structured data
    print("\n  [Uploading structured CSV data]")
    cluster.put(os.path.join(BASE, "data", "transactions_2023_01.csv"),
                "/wallet/raw/transactions/year=2023/month=01/part-0.csv")
    cluster.put(os.path.join(BASE, "data", "transactions_2023_02.csv"),
                "/wallet/raw/transactions/year=2023/month=02/part-0.csv")
    cluster.put(os.path.join(BASE, "data", "users.csv"),
                "/wallet/raw/users/users.csv")

    # Unstructured data (simulate log file)
    log_path = os.path.join(HDFS_ROOT, "app_logs.txt")
    with open(log_path, "w") as f:
        for i in range(500):
            f.write(f"[2023-01-{(i%28)+1:02d} {i%24:02d}:00:00] TXN{str(i).zfill(8)} "
                    f"PROCESSED amount={i*100} status=SUCCESS\n")
    cluster.put(log_path, "/wallet/raw/logs/app_logs_2023_01.txt")

    cluster.ls("/wallet/")
    cluster.fsck()
    print("\nTask 12 complete ✓")
    return cluster


# ══════════════════════════════════════════════════════════════
# Task 13 – Architecture explanation
# ══════════════════════════════════════════════════════════════
def task13(cluster: HDFSCluster):
    print("\n" + "=" * 60)
    print("  Task 13 – HDFS Architecture: NameNode & DataNode")
    print("=" * 60)

    print("""
  ┌──────────────────────────────────────────────────────┐
  │                   HDFS ARCHITECTURE                  │
  │                                                      │
  │  Client ─────────────────────────────────────┐       │
  │                                              ▼       │
  │           ┌─────────────────────┐                    │
  │           │      NameNode       │  ← MASTER          │
  │           │  (namespace + meta) │                    │
  │           └──────┬──────────────┘                    │
  │                  │  block locations                  │
  │       ┌──────────┼──────────┐                        │
  │       ▼          ▼          ▼                        │
  │  ┌────────┐ ┌────────┐ ┌────────┐                    │
  │  │ DataN1 │ │ DataN2 │ │ DataN3 │  ← WORKERS         │
  │  │ blk_0  │ │ blk_0  │ │ blk_1  │  (3× replica)      │
  │  │ blk_1  │ │ blk_2  │ │ blk_2  │                    │
  │  └────────┘ └────────┘ └────────┘                    │
  └──────────────────────────────────────────────────────┘
    """)

    nn_meta = cluster.namenode.namespace
    sample_key = list(nn_meta.keys())[0]
    sample = nn_meta[sample_key]

    print("  [NameNode metadata for first uploaded file]")
    print(f"  HDFS path    : {sample['hdfs_path']}")
    print(f"  Total bytes  : {sample['total_bytes']:,}")
    print(f"  Num blocks   : {sample['num_blocks']}")
    print(f"  Replication  : {sample['replication']}×")
    print(f"\n  Block map:")
    for blk in sample["blocks"]:
        print(f"    {blk['block_id']}  {blk['size']:>8,} B  → {blk['replicas']}")

    print("""
  [Key Concepts]

  NameNode (Master)
  • Stores filesystem namespace tree (directories + file-to-block mapping)
  • Holds block locations in memory – NOT on disk (rebuilt from DataNode reports)
  • Single point of failure → mitigated by Secondary NameNode / HA NameNode
  • Edit log  : every metadata change appended here
  • FsImage   : periodic checkpoint of entire namespace

  DataNode (Workers)
  • Store actual data as fixed-size blocks (default 128 MB)
  • Send heartbeat to NameNode every 3 seconds
  • Send block report every 6 hours
  • Handle parallel reads/writes from HDFS clients

  Write flow  : Client → NameNode (get DN list) → DN1 → DN2 → DN3 (pipeline)
  Read  flow  : Client → NameNode (get nearest DN) → DataNode → Client
  Fault tol.  : If a DN dies, NameNode re-replicates its blocks on surviving DNs
    """)

    # Verify round-trip read
    print("  [Verification: HDFS get round-trip]")
    retrieved = cluster.get("/wallet/raw/users/users.csv")
    local_size = os.path.getsize(os.path.join(BASE, "data", "users.csv"))
    ok = len(retrieved) == local_size
    print(f"  Original size : {local_size:,} bytes")
    print(f"  Retrieved size: {len(retrieved):,} bytes")
    print(f"  Match         : {'✓ YES' if ok else '✗ NO'}")

    print("\nTask 13 complete ✓")


if __name__ == "__main__":
    cluster = task12()
    task13(cluster)
