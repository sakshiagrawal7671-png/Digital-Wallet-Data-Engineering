"""
Task 02 – Networking
Simulates data transfer between systems using APIs.
Analyses HTTP/HTTPS/FTP protocols, demonstrates packet flow,
and explains how digital wallet data moves securely.
"""
import json, hashlib, socket, time
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# 1. Protocol analysis
# ─────────────────────────────────────────────────────────────
PROTOCOLS = {
    "HTTP  (port 80)": {
        "encrypted": False,
        "wallet_use": "Internal health-check endpoints only",
        "risk": "Data visible in transit – NEVER for transactions",
    },
    "HTTPS (port 443)": {
        "encrypted": True,
        "wallet_use": "ALL payment & auth API calls",
        "risk": "TLS 1.3 – safe for financial data",
    },
    "FTP   (port 21)": {
        "encrypted": False,
        "wallet_use": "Legacy batch file transfers (use SFTP instead)",
        "risk": "Credentials and data are plain-text",
    },
    "SFTP  (port 22)": {
        "encrypted": True,
        "wallet_use": "Bank reconciliation files, settlement CSVs",
        "risk": "SSH-encrypted – safe for bulk files",
    },
    "WebSocket (wss)": {
        "encrypted": True,
        "wallet_use": "Real-time balance updates & fraud alerts",
        "risk": "TLS-wrapped – low-latency and safe",
    },
}


def print_protocol_analysis():
    print("\n" + "=" * 62)
    print("  Task 02 – Protocol Analysis for Digital Wallet System")
    print("=" * 62)
    for proto, info in PROTOCOLS.items():
        lock = "🔒" if info["encrypted"] else "🔓"
        print(f"\n  {lock} {proto}")
        print(f"     Wallet use : {info['wallet_use']}")
        print(f"     Security   : {info['risk']}")


# ─────────────────────────────────────────────────────────────
# 2. Simulate an HTTPS POST request (no external call needed)
# ─────────────────────────────────────────────────────────────
def simulate_https_post(transaction: dict) -> dict:
    """Simulates what happens when the wallet app POSTs a transaction."""
    payload   = json.dumps(transaction).encode()
    req_id    = hashlib.md5(str(time.time()).encode()).hexdigest()[:12]
    signature = hashlib.sha256(payload).hexdigest()

    request = {
        "method":  "POST",
        "url":     "https://api.walletpay.in/v1/transactions",
        "headers": {
            "Content-Type":    "application/json",
            "Authorization":   f"Bearer eyJhbGc.{req_id}.sig",
            "X-Request-ID":    req_id,
            "X-Payload-Hash":  signature[:16],
            "User-Agent":      "WalletApp/3.1.0 (Android)",
        },
        "body_bytes": len(payload),
        "tls_version": "TLS 1.3",
        "cipher":      "TLS_AES_256_GCM_SHA384",
    }

    # Simulated server response
    response = {
        "status": 200,
        "body": {"txn_id": transaction["txn_id"], "status": "ACCEPTED",
                 "timestamp": datetime.now().isoformat()},
    }
    return request, response


# ─────────────────────────────────────────────────────────────
# 3. Simulate packet capture
# ─────────────────────────────────────────────────────────────
def simulate_packet(txn: dict) -> dict:
    src_port = 49152 + (hash(txn["txn_id"]) % 16383)
    return {
        "network": {
            "src_ip":  f"192.168.1.{hash(txn['user_id']) % 254 + 1}",
            "dst_ip":  "54.240.193.10",   # AWS endpoint (simulated)
            "ttl":     64,
            "proto":   "TCP",
        },
        "transport": {
            "src_port": src_port,
            "dst_port": 443,
            "flags":    "PSH+ACK",
            "seq":      hash(txn["txn_id"]) % 1_000_000,
        },
        "application": {
            "protocol":  "HTTPS",
            "method":    "POST /v1/transactions",
            "encrypted": True,
            "payload_B": len(json.dumps(txn)),
        },
    }


# ─────────────────────────────────────────────────────────────
# 4. DNS resolution demo
# ─────────────────────────────────────────────────────────────
def dns_demo():
    try:
        ip = socket.gethostbyname("www.google.com")
    except Exception:
        ip = "142.250.x.x (simulated)"
    print(f"\n  DNS  www.google.com  →  {ip}")
    print("  TCP socket lifecycle:")
    for step in [
        "socket(AF_INET, SOCK_STREAM)   # create socket",
        "connect(('api.walletpay.in', 443))  # 3-way handshake",
        "send(tls_encrypted_payload)    # POST transaction",
        "recv(4096)                     # read 200 OK response",
        "close()                        # teardown",
    ]:
        print(f"    >>> {step}")


# ─────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sample_txn = {
        "txn_id":   "TXN00000001",
        "user_id":  "U0001",
        "merchant": "Amazon",
        "amount":   1299.99,
        "currency": "INR",
        "status":   "PENDING",
    }

    print_protocol_analysis()

    print("\n\n--- Simulated HTTPS POST ---")
    req, resp = simulate_https_post(sample_txn)
    print("  REQUEST HEADERS:")
    for k, v in req["headers"].items():
        print(f"    {k}: {v}")
    print(f"  Body size : {req['body_bytes']} bytes")
    print(f"  TLS       : {req['tls_version']} / {req['cipher']}")
    print(f"\n  RESPONSE  : {resp['status']} {json.dumps(resp['body'])}")

    print("\n--- Simulated Packet Capture ---")
    pkt = simulate_packet(sample_txn)
    print(json.dumps(pkt, indent=4))

    dns_demo()

    print("\n\n--- Secure Data Flow (step-by-step) ---")
    steps = [
        ("1 User initiates",     "Tap 'Pay' → app creates TXN_ID locally"),
        ("2 JWT attached",       "Bearer token added to Authorization header"),
        ("3 TLS handshake",      "Client ↔ Server exchange certificates"),
        ("4 Payload encrypted",  "AES-256-GCM encrypts JSON body"),
        ("5 HTTPS POST",         "Packet sent to /v1/transactions"),
        ("6 Server validates",   "Signature + token verified server-side"),
        ("7 DB write",           "Transaction persisted to PostgreSQL (ACID)"),
        ("8 200 OK response",    "Receipt JSON returned to client"),
        ("9 Push notification",  "WebSocket pushes balance update to app"),
        ("10 Audit log",         "Immutable log entry written to S3"),
    ]
    for step, desc in steps:
        print(f"  {step:<22} →  {desc}")

    print("\nTask 02 complete ✓")
