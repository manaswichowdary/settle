"""Crash test for Settle.

Sends payments to the API from many threads while killing services at random:
a worker every few seconds, and the API itself a few times. Clients that lose a request
retry with the same idempotency key, like a real payment client would. At the end it waits for
the ledger to catch up and checks that every accepted payment was applied exactly once.

Usage:  python scripts/crash_test.py --payments 20000
Needs only Python 3 and the running Docker Compose stack.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPException
from pathlib import Path

API = "http://localhost:8080"
lock = threading.Lock()
stats = {"succeeded": 0, "created": 0, "replayed": 0, "retries_after_error": 0, "client_duplicate_sends": 0,
         "worker_kills": 0, "api_kills": 0, "failed": 0}
lag_samples: list[int] = []
stop = threading.Event()


def bump(key: str, n: int = 1) -> None:
    with lock:
        stats[key] += n


def http(method: str, path: str, body: dict | None = None, key: str | None = None, timeout: float = 10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Idempotency-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def compose(*args: str) -> str:
    return subprocess.run(["docker", "compose", *args], capture_output=True, text=True, check=True).stdout


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], capture_output=True, text=True)


def send_payment(i: int, accounts: list[str], resend_share: float) -> None:
    key = str(uuid.uuid4())
    a, b = random.sample(accounts, 2)
    body = {"fromAccount": a, "toAccount": b, "amountCents": random.randint(1, 50_000), "currency": "USD"}
    sends = 2 if random.random() < resend_share else 1      # some clients send the same request twice
    for attempt in range(sends):
        deadline = time.time() + 120
        while True:
            try:
                status, _ = http("POST", "/payments", body, key)
                if status in (200, 201):
                    # 200 on the first send means an earlier try was saved but its response was lost
                    bump("created" if status == 201 else "replayed")
                    if attempt == 0:
                        bump("succeeded")
                    if attempt == 1:
                        bump("client_duplicate_sends")
                    break
                if status >= 500:
                    raise ConnectionError(status)
                bump("failed")
                return
            except (urllib.error.URLError, HTTPException, ConnectionError, TimeoutError, OSError, ValueError):
                if time.time() > deadline:
                    bump("failed")
                    return
                bump("retries_after_error")      # the API may be down: retry the same key
                time.sleep(0.5)


def chaos(worker_every: float, api_kills_at: list[float], start: float) -> None:
    """Kill a random worker every few seconds (and bring it back), and kill the API at set times."""
    pending_api = sorted(api_kills_at)
    while not stop.is_set():
        time.sleep(worker_every)
        if stop.is_set():
            break
        workers = compose("ps", "-q", "worker").split()
        if workers:
            victim = random.choice(workers)
            docker("kill", victim)
            bump("worker_kills")
            time.sleep(1.5)
            docker("start", victim)
        if pending_api and time.time() - start >= pending_api[0]:
            pending_api.pop(0)
            api = compose("ps", "-q", "api").split()
            if api:
                docker("kill", api[0])
                bump("api_kills")
                time.sleep(1.5)
                docker("start", api[0])


def sample_lag() -> None:
    while not stop.is_set():
        try:
            _, check = http("GET", "/admin/check", timeout=5)
            lag_samples.append(int(check["payments"]) - int(check["events_applied"]))
        except Exception:
            pass
        time.sleep(1)


def wait_healthy(seconds: int = 180) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            status, body = http("GET", "/actuator/health", timeout=3)
            if status == 200 and body.get("status") == "UP":
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit("The API did not become healthy. Is the stack running? Try: make up")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payments", type=int, default=20_000)
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--accounts", type=int, default=100)
    ap.add_argument("--resend-share", type=float, default=0.05, help="share of clients that send twice")
    ap.add_argument("--worker-kill-every", type=float, default=4.0, help="seconds between worker kills")
    ap.add_argument("--api-kills", type=int, default=2)
    ap.add_argument("--out", default="results/crash_test.json")
    args = ap.parse_args()

    wait_healthy()
    _, before = http("GET", "/admin/check")
    accounts = [f"acct-{i:03d}" for i in range(args.accounts)]
    print(f"Sending {args.payments} payments from {args.threads} threads while killing services...")

    start = time.time()
    expected = max(10.0, args.payments / 400)          # rough run length, used to space out API kills
    api_times = [expected * (k + 1) / (args.api_kills + 1) for k in range(args.api_kills)]
    threads = [threading.Thread(target=chaos, args=(args.worker_kill_every, api_times, start), daemon=True),
               threading.Thread(target=sample_lag, daemon=True)]
    for t in threads:
        t.start()
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        list(pool.map(lambda i: send_payment(i, accounts, args.resend_share), range(args.payments)))
    send_seconds = time.time() - start
    stop.set()

    print("All requests answered. Waiting for the ledger to catch up...")
    deadline, check = time.time() + 300, {}
    while time.time() < deadline:
        try:
            _, check = http("GET", "/admin/check")
            if check.get("outbox_unpublished") == 0 and check.get("payments") == check.get("events_applied"):
                break
        except Exception:
            pass
        time.sleep(2)
    _, check = http("GET", "/admin/check")

    new_payments = int(check["payments"]) - int(before["payments"])
    result = {
        "payments_requested": args.payments,
        "payments_succeeded": stats["succeeded"],
        "new_rows_in_payments_table": new_payments,
        "requests_replayed_with_same_key": stats["replayed"],
        "client_duplicate_sends": stats["client_duplicate_sends"],
        "retries_after_connection_error": stats["retries_after_error"],
        "failed_requests": stats["failed"],
        "worker_kills": stats["worker_kills"],
        "api_kills": stats["api_kills"],
        "lost_payments": int(check["payments_not_applied"]),
        "payments_applied_more_than_once": int(check["payments_with_wrong_entry_count"]),
        "duplicate_deliveries_skipped": int(check["duplicate_deliveries_skipped"]) - int(before["duplicate_deliveries_skipped"]),
        "ledger_sum_cents": int(float(check["ledger_sum_cents"])),
        "balance_mismatches": int(check["balance_mismatches"]),
        "send_seconds": round(send_seconds, 1),
        "payments_per_second": round(stats["succeeded"] / send_seconds, 1),
        "max_consumer_lag": max(lag_samples) if lag_samples else None,
        "apply_latency_ms": check["apply_latency_ms"],
        "ledger_ok": check["ok"],
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=1))
    print(json.dumps(result, indent=1))
    print(f"\nSaved to {args.out}")
    ok = (result["ledger_ok"] and result["lost_payments"] == 0 and result["payments_applied_more_than_once"] == 0
          and new_payments == stats["succeeded"] and stats["failed"] == 0)
    print("PASS: nothing lost, nothing applied twice." if ok else "FAIL: see the numbers above.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
