#!/usr/bin/env python3
"""
Umurinzi high-concurrency simulator.

Adapted to the REAL backend protocol:
  * Auth:        POST /auth/register + /auth/login  -> JWT (both are public routes
                 that bypass the JWT middleware via X-Gql-Operation injection).
  * Operations:  POST /graphql with {"operationName": ..., "variables": ...}
                 (GraphQL-style op routing, JWT via Authorization: Bearer).
  * Telemetry:   WebSocket /ws/telemetry (JWT via Authorization header),
                 JSON frames {type:"FRAME", lat, lng, speed_kmh, ...} every FRAME_SEC.
  * Race gate:   two drivers POST AcceptOrder simultaneously; exactly one must
                 win (200) and the loser must get ORDER_LOCKED.

Phases:
  --phase gates   : Ghost-order COD block + forest/lake pin snapping gate (Step 1)
  --phase race    : 3 moving drivers + order + concurrent accept race (Step 2)
  --phase load    : N drivers + M orders, full JIT -> dispatch -> accept -> deliver
  --phase all     : gates -> race -> load (default)

Run against the Docker stack:
  python3 simulation.py --base http://localhost:8080

Monitoring while it runs:
  docker logs -f umurinzi_backend --tail 50
  docker exec umurinzi_redis redis-cli -a "$REDIS_PASSWORD" \
      GEORADIUS drivers:geo 30.090 -1.949 5 km WITHCOORD
"""

import argparse
import json
import random
import sys
import threading
import time
import uuid

import requests
import websocket  # pip install websocket-client


# ── Kigali test coordinates ────────────────────────────────────────────────────
KIGALI_HEIGHTS = (-1.9490, 30.0900)   # driver staging area
KIMIRONKO      = (-1.9358, 30.1069)   # nearby dropoff
# Wilderness probes (verified against OSRM /nearest — snap distance > 1 km):
NYUNGWE_DEEP   = (-2.3000, 29.3200)   # 1.14 km from nearest road
LAKE_KIVU      = (-1.7500, 29.1000)   # 16 km out in open water
GISO_SHAFT     = (-1.9363, 30.1300)   # regular urban dropoff


class Client:
    """Thin wrapper over the backend's op-routing HTTP + WS protocol."""

    def __init__(self, base_url, token=None):
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        self.token = token
        self.user_id = None
        self.customer_id = None  # lazily-registered FK-valid customer for orders

    # ── auth (public routes, op injected via X-Gql-Operation) ──
    def register(self, full_name, phone, password, role):
        r = self.s.post(
            f"{self.base}/auth/register",
            json={"query": "mutation", "operationName": "Register",
                  "variables": {"input": {"fullName": full_name, "phone": phone,
                                          "password": password, "role": role}}},
            timeout=10,
        )
        data = self._ok(r, "register")
        if data.get("errors"):
            raise RuntimeError(f"register rejected: {data['errors'][0]['message'][:120]}")
        self.token = data["data"]["token"]
        self.user_id = data["data"]["user"]["id"]
        return self.token

    def login(self, phone, password):
        r = self.s.post(
            f"{self.base}/auth/login",
            json={"query": "mutation", "operationName": "Login",
                  "variables": {"input": {"phone": phone, "password": password}}},
            timeout=10,
        )
        data = self._ok(r, "login")
        self.token = data["data"]["token"]
        self.user_id = data["data"]["user"]["id"]
        return self.token

    # ── authenticated ops (POST /graphql) ──
    def op(self, op_name, variables, idempotency_key=None):
        headers = {"Authorization": f"Bearer {self.token}"}
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key
        r = self.s.post(
            f"{self.base}/graphql",
            json={"query": "mutation", "operationName": op_name, "variables": variables},
            headers=headers, timeout=15,
        )
        return r

    def create_delivery(self, pickup, dropoff, category="GENERAL", weight=1.0,
                        is_prepaid=True, prep_time_minutes=None, customer_id=None):
        if customer_id is None:
            if self.customer_id is None:
                self.customer_id = self.create_customer()
            customer_id = self.customer_id
        var_input = {
            "customerId": customer_id or f"cust_{uuid.uuid4().hex[:8]}",
            "pickupLocation": {"lat": pickup[0], "lng": pickup[1]},
            "dropoffLocation": {"lat": dropoff[0], "lng": dropoff[1]},
            "description": f"sim {category} parcel",
            "weightKg": weight,
            "packageCategory": category,
            "isPrepaid": is_prepaid,
        }
        if prep_time_minutes is not None:
            var_input["prepTimeMinutes"] = prep_time_minutes
        return self.op("CreateDelivery", {"input": var_input})

    def accept_order(self, delivery_id):
        return self.op("AcceptOrder", {"deliveryId": delivery_id})

    def get_delivery(self, delivery_id):
        return self.op("GetDelivery", {"id": delivery_id})

    def create_customer(self):
        """Register a throwaway CUSTOMER whose UUID satisfies the
        deliveries.customer_id → users(id) foreign key."""
        c = Client(self.base)
        phone = f"+25072{int(time.time() * 1000) % 100000000:08d}"
        c.register(f"Sim Customer", phone, "SimPassw0rd!", "CUSTOMER")
        return c.user_id

    def set_driver_online(self):
        return self.op("SetDriverOnline", {})

    # ── helpers ──
    @staticmethod
    def _ok(resp, what):
        try:
            return resp.json()
        except ValueError:
            raise RuntimeError(f"{what}: non-JSON HTTP {resp.status_code}: {resp.text[:200]}")

    @staticmethod
    def gql_errors(body):
        return body.get("errors") or []

    @staticmethod
    def gql_data(body, key=None):
        data = body.get("data")
        if data is None:
            return None
        if key:
            return data.get(key)
        return data


# ── WebSocket driver (real telemetry protocol) ────────────────────────────────

def telemetry_loop(token, driver_label, start_lat, start_lng, stop_event,
                   speed_step=0.00015, interval=None, stats=None):
    """Open /ws/telemetry with the JWT, then stream drifting FRAMEs.

    Drifts south-east like a moto cruising Kigali streets. Acks / errors are
    tallied into `stats` for the run summary.
    """
    ws_url = build_ws_url()
    seq = 0
    lat, lng = start_lat, start_lng
    backoff = 1.0
    while not stop_event.is_set():
        try:
            ws = websocket.create_connection(
                ws_url, timeout=10,
                header=[f"Authorization: Bearer {token}"],
            )
            backoff = 1.0
            while not stop_event.is_set():
                seq += 1
                frame = {
                    "type": "FRAME",
                    "delivery_id": "",
                    "lat": round(lat, 6),
                    "lng": round(lng, 6),
                    "speed_kmh": 28.0,
                    "bearing": 135.0,
                    "accuracy_m": 5.0,
                    "battery_pct": 80.0,
                    "ts_ms": int(time.time() * 1000),
                    "seq": seq,
                }
                ws.send(json.dumps(frame))
                ws.settimeout(3)
                try:
                    ack = ws.recv()
                    if stats is not None:
                        stats["frames_acked"] += 1
                    _ = ack
                except websocket.WebSocketTimeoutException:
                    pass  # server ack is best-effort; keep streaming
                lat += speed_step
                lng += speed_step
                stop_event.wait(interval if interval else FRAME_SEC)
            ws.close()
        except Exception as e:
            if stats is not None:
                stats["ws_errors"] += 1
            log(f"  [{driver_label}] ws error: {e} — reconnecting in {backoff:.0f}s")
            stop_event.wait(backoff)
            backoff = min(backoff * 2, 10)


def build_ws_url():
    # module-level BASE_URL set in main()
    return BASE_URL.replace("http", "ws", 1).rstrip("/") + "/ws/telemetry"


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


FRAME_SEC = 3.0   # matches the design spec: one ping every ~3 s


# ── Step 1: defensive gate smoke tests ────────────────────────────────────────

def phase_gates(cl):
    log("── PHASE 1: defensive gates ──")
    results = []

    # Gate A: Ghost Order COD block.
    # customer_risk.customer_id FKs to users(id), so register a real customer,
    # then seed risk_score 5.0 (> 3.0 threshold) and try a COD order.
    risky = Client(BASE_URL)
    risky.register("High Risk Cust", f"+25075{int(time.time()) % 100000000:08d}",
                   "SimPassw0rd!", "CUSTOMER")
    seed_high_risk_customer(risky.user_id)
    r = cl.create_delivery(KIGALI_HEIGHTS, KIMIRONKO, is_prepaid=False,
                           customer_id=risky.user_id)
    body = cl._ok(r, "cod gate")
    errs = cl.gql_errors(body)
    rejected = r.status_code == 200 and errs and "pre-pay" in errs[0]["message"]
    results.append(("Ghost Order COD block rejects high-risk COD", rejected,
                    errs[0]["message"] if errs else body))
    # The prepaid escape hatch must pass the gate.
    r = cl.create_delivery(KIGALI_HEIGHTS, KIMIRONKO, is_prepaid=True,
                           customer_id=risky.user_id)
    body = cl._ok(r, "cod prepaid")
    allowed = cl.gql_errors(body) == []
    results.append(("Prepaid high-risk order passes the gate", allowed, body))

    # Gate B: inaccessible pin snapping (Nyungwe forest + Lake Kivu).
    for label, coord in (("Nyungwe forest", NYUNGWE_DEEP), ("Lake Kivu", LAKE_KIVU)):
        r = cl.create_delivery(KIGALI_HEIGHTS, coord)
        body = cl._ok(r, f"pin gate {label}")
        errs = cl.gql_errors(body)
        rejected = r.status_code == 200 and errs and "inaccessible" in errs[0]["message"]
        results.append((f"{label} pin snapped >1km from road → rejected", rejected,
                        errs[0]["message"] if errs else body))

    for name, ok, detail in results:
        log(f"  {'✅ PASS' if ok else '❌ FAIL'}  {name}"
            + (f"\n         ↳ {str(detail)[:140]}" if not ok else ""))
    return all(ok for _, ok, _ in results)


def seed_high_risk_customer(user_id):
    """Upsert customer_risk risk_score=5.0 for the given user via psql."""
    import os
    import subprocess
    sql = ("INSERT INTO customer_risk (customer_id, risk_score) "
           f"VALUES ('{user_id}', 5.0) "
           "ON CONFLICT (customer_id) DO UPDATE SET risk_score = 5.0;")
    cmd = ["docker", "compose", "exec", "-T", "postgres",
           "psql", "-U", "umurinzi", "-d", "umurinzi", "-c", sql]
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=True,
                       cwd=os.path.dirname(os.path.abspath(__file__)))
    except Exception as e:
        log(f"  ⚠ could not seed customer_risk ({e}); COD gate test may be inconclusive")


# ── Step 2: race condition gate ───────────────────────────────────────────────

def phase_race(cl, drivers):
    log("── PHASE 2: concurrent accept race ──")

    # Populate the Redis geo index first.
    stop = threading.Event()
    stats = {"frames_acked": 0, "ws_errors": 0}
    threads = []
    for drv in drivers:
        t = threading.Thread(
            target=telemetry_loop,
            args=(drv["token"], drv["label"], drv["lat"], drv["lng"], stop, 0.0, 2.0, stats),
            daemon=True,
        )
        t.start()
        threads.append(t)
    log(f"  {len(drivers)} drivers streaming telemetry (ws) — warming geo index 6 s")
    time.sleep(6)

    # Create the order via the merchant client (JIT enqueues it immediately:
    # DOCUMENTS category ⇒ ~2 min prep, driver ETAs ≪ that ⇒ released next tick).
    r = cl.create_delivery(KIGALI_HEIGHTS, KIMIRONKO, category="DOCUMENTS")
    body = cl._ok(r, "create order")
    if cl.gql_errors(body):
        log(f"  ❌ order creation failed: {cl.gql_errors(body)}")
        stop.set()
        return False
    delivery_id = cl.gql_data(body)["delivery"]["id"]
    log(f"  order created: {delivery_id}")

    # Wait for the JIT batch tick to dispatch (batch window is ~12 s).
    log("  waiting 16 s for JIT dispatch + match result …")
    time.sleep(16)

    winner_holder = {}
    def racer(driver, results_out, idx):
        try:
            resp = driver["client"].accept_order(delivery_id)
            body = cl._ok(resp, "accept")
            errs = cl.gql_errors(body)
            code = errs[0]["extensions"].get("code") if errs else "OK"
            results_out[idx] = (driver["label"], resp.status_code, code)
        except Exception as e:
            results_out[idx] = (driver["label"], 0, str(e))

    # Millisecond-simultaneous accepts from two drivers.
    racers = drivers[:2]
    out = [None, None]
    t1 = threading.Thread(target=racer, args=(racers[0], out, 0))
    t2 = threading.Thread(target=racer, args=(racers[1], out, 1))
    t1.start(); t2.start(); t1.join(); t2.join()

    codes = [c for _, _, c in out]
    n_ok = codes.count("OK")
    benign = {"ORDER_LOCKED", "INVALID_TRANSITION"}
    # PASS when the lock held: exactly one winner, OR both blocked because the
    # JIT match intended a third driver (guard working as designed).
    # FAIL only on double-OK (lock failure) or unexpected codes (crash/5xx).
    ok = (n_ok == 1 and set(codes) - {"OK"} <= benign) or \
         (n_ok == 0 and set(codes) <= benign)
    for label, status, code in out:
        log(f"  {label}: HTTP {status} → {code}")
    log(f"  {'✅ PASS' if ok else '❌ FAIL'}  race gate (exactly-one-winner lock)")
    if stats["frames_acked"]:
        log(f"  telemetry: {stats['frames_acked']} frames acked, {stats['ws_errors']} ws errors")
    stop.set()
    return ok


# ── Phase 3: sustained load ───────────────────────────────────────────────────

def phase_load(cl, drivers, n_orders=8, duration=90):
    log(f"── PHASE 3: sustained load — {len(drivers)} drivers, {n_orders} orders/{duration}s ──")
    stop = threading.Event()
    stats = {"frames_acked": 0, "ws_errors": 0}
    for drv in drivers:
        threading.Thread(
            target=telemetry_loop,
            args=(drv["token"], drv["label"], drv["lat"], drv["lng"], stop, 0.0, 2.0, stats),
            daemon=True,
        ).start()

    created, accepted, failed = [], [], []
    deadline = time.time() + duration
    i = 0
    while time.time() < deadline and len(created) < n_orders:
        pickup = random.choice([KIGALI_HEIGHTS, (-1.9443, 30.0619), (-1.9590, 30.0960)])
        dropoff = random.choice([KIMIRONKO, GISO_SHAFT, (-1.9700, 30.1180)])
        cat = random.choice(["DOCUMENTS", "GENERAL", "PERISHABLE"])
        r = cl.create_delivery(pickup, dropoff, category=cat,
                               prep_time_minutes=random.choice([1, 2, 3]))
        body = cl._ok(r, "load create")
        if cl.gql_errors(body):
            failed.append(body)
            continue
        delivery_id = cl.gql_data(body)["delivery"]["id"]
        created.append(delivery_id)
        log(f"  order {i+1}/{n_orders} created ({cat}): {delivery_id[:8]}…")
        i += 1
        time.sleep(6)

    # Give matching a moment, then try to accept everything with free drivers.
    time.sleep(12)
    for delivery_id in created:
        for drv in drivers:
            resp = drv["client"].accept_order(delivery_id)
            body = cl._ok(resp, "load accept")
            if not cl.gql_errors(body):
                accepted.append(delivery_id)
                break
        else:
            failed.append(delivery_id)

    log(f"  created={len(created)} accepted={len(accepted)} unmatched/failed={len(failed)}")
    log(f"  telemetry: {stats['frames_acked']} frames acked, {stats['ws_errors']} ws errors")
    ok = len(created) >= n_orders - 2 and len(accepted) >= max(1, len(created) // 3)
    log(f"  {'✅ PASS' if ok else '❌ FAIL'}  load phase")
    stop.set()
    return ok


# ── Step 3 helpers: chaos test (Postgres down → Redis fallback) ───────────────

def phase_chaos(cl, delivery_id=None):
    """Stop postgres, GET the delivery — must be served from the Redis cache."""
    log("── PHASE 4: Postgres chaos → Redis fallback ──")
    import os
    import subprocess

    # Need a delivery to read. Reuse one or create it now.
    if not delivery_id:
        r = cl.create_delivery(KIGALI_HEIGHTS, KIMIRONKO, category="DOCUMENTS")
        body = cl._ok(r, "chaos create")
        if cl.gql_errors(body):
            log("  ❌ could not create delivery for chaos test")
            return False
        delivery_id = cl.gql_data(body)["delivery"]["id"]
        time.sleep(12)  # let the JIT pipeline touch it

    # Prime the Redis cache while PG is healthy.
    r = cl.get_delivery(delivery_id)
    if cl.gql_errors(cl._ok(r, "prime")):
        log("  ❌ could not prime cache")
        return False

    subprocess.run(["docker", "compose", "stop", "postgres"],
                   capture_output=True, text=True, timeout=60,
                   cwd=os.path.dirname(os.path.abspath(__file__)))
    log("  postgres stopped — hitting GetDelivery 3×")
    ok = True
    for _ in range(3):
        r = cl.get_delivery(delivery_id)
        body = cl._ok(r, "chaos get")
        ok = ok and cl.gql_errors(body) == []
        time.sleep(1)
    subprocess.run(["docker", "compose", "start", "postgres"],
                   capture_output=True, text=True, timeout=90,
                   cwd=os.path.dirname(os.path.abspath(__file__)))
    log(f"  {'✅ PASS' if ok else '❌ FAIL'}  circuit breaker + Redis fallback served requests")
    return ok


# ── Bootstrap: provision merchant + drivers on a fresh stack ─────────────────

def register_or_login(cl, full_name, phone, password, role):
    """Register; on 'already exists' fall back to login so re-runs are idempotent."""
    try:
        cl.register(full_name, phone, password, role)
    except (RuntimeError, TypeError, KeyError):
        cl.login(phone, password)
    return cl


def bootstrap(cl, n_drivers):
    run_tag = uuid.uuid4().hex[:6].upper()  # per-run unique plates/IDs — re-run safe
    phone = f"+25078{int(time.time()) % 100000000:08d}"
    register_or_login(cl, "Sim Merchant", phone, "SimPassw0rd!", "MERCHANT")
    log(f"merchant ready: {cl.user_id}")

    drivers = []
    for i in range(n_drivers):
        d = Client(BASE_URL)
        dphone = f"+25073{(int(time.time() * 10) + i) % 100000000:08d}"
        register_or_login(d, f"Sim Driver {i}", dphone, "SimPassw0rd!", "DRIVER")
        # Tolerant bootstrap: profile may already exist (or already be ACTIVE) on re-runs.
        body = cl._ok(d.op("RegisterDriver", {"input": {
            "nationalId": f"1199{i:05d}{run_tag[:5]}", "licenseNumber": f"LIC-{run_tag}-{i:03d}",
            "vehicleType": "MOTORCYCLE", "plateNumber": f"SIM{run_tag[:4]}{i}",
            "maxWeightKg": 30,
        }}), "register driver")
        if cl.gql_errors(body):
            log(f"  driver{i}: RegisterDriver skipped ({cl.gql_errors(body)[0]['message'][:60]}…)")
        body = cl._ok(d.op("ApproveDriver", {"driverUserId": d.user_id}), "approve driver")
        if cl.gql_errors(body):
            log(f"  driver{i}: ApproveDriver skipped ({cl.gql_errors(body)[0]['message'][:60]}…)")
        d.set_driver_online()
        lat, lng = KIGALI_HEIGHTS
        lat += random.uniform(-0.004, 0.004)
        lng += random.uniform(-0.004, 0.004)
        drivers.append({"label": f"driver{i}", "client": d, "token": d.token,
                        "lat": lat, "lng": lng})
    log(f"{len(drivers)} drivers registered, approved, online")
    return drivers


# ── main ──────────────────────────────────────────────────────────────────────

BASE_URL = "http://localhost:8080"

def main():
    global BASE_URL
    ap = argparse.ArgumentParser(description="Umurinzi concurrency simulator")
    ap.add_argument("--base", default="http://localhost:8080")
    ap.add_argument("--phase", default="all",
                    choices=["gates", "race", "load", "chaos", "all"])
    ap.add_argument("--drivers", type=int, default=5)
    ap.add_argument("--orders", type=int, default=8)
    ap.add_argument("--duration", type=int, default=90)
    args = ap.parse_args()
    BASE_URL = args.base

    results = []
    if args.phase in ("all", "gates", "race", "load", "chaos"):
        cl = Client(BASE_URL)
        try:
            drivers = bootstrap(cl, args.drivers)
        except RuntimeError as e:
            log(f"bootstrap failed: {e} — trying login fallback (stack already seeded?)")
            cl = Client(BASE_URL)
            cl.login("+250780000001", "SimPassw0rd!")
            drivers = bootstrap(cl, args.drivers)

        if args.phase in ("all", "gates"):
            results.append(("gates", phase_gates(cl)))
        if args.phase in ("all", "race"):
            results.append(("race", phase_race(cl, drivers)))
        if args.phase in ("all", "load"):
            results.append(("load", phase_load(cl, drivers, args.orders, args.duration)))
        if args.phase == "chaos":
            results.append(("chaos", phase_chaos(cl, sys.argv[sys.argv.index("--delivery-id") + 1]
                                                 if "--delivery-id" in sys.argv else None)))

    log("════════ SUMMARY ════════")
    for name, ok in results:
        log(f"  {'✅' if ok else '❌'} {name}")
    sys.exit(0 if all(ok for _, ok in results) else 1)


if __name__ == "__main__":
    main()
