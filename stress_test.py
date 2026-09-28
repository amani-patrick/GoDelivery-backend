#!/usr/bin/env python3
"""
Umurinzi async stress harness — thousands of virtual users, one event loop.

Models the full marketplace loop per driver:
    register -> profile -> approve -> online -> WS telemetry stream
    -> receive DISPATCH alert -> AcceptOrder -> ConfirmPickup(QR)
    -> ConfirmDelivery(PIN) -> driver recycled -> repeat

A merchant coroutine blasts orders (round-robin over pre-registered customers)
at a fixed interval; every order is tracked from CreateDelivery to
ConfirmDelivery to produce end-to-end latency percentiles.

Designed for asyncio so 5000 concurrent WebSocket connections stay cheap
(threads would need 5000 OS threads and die around ~1k).

Usage:
  python3 stress_test.py --users 500  --duration 120
  python3 stress_test.py --users 5000 --duration 300 --order-interval 0.5

Requires: pip install aiohttp websockets   (already present)
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
import uuid

import aiohttp
import websockets

# ── Kigali test coordinates ────────────────────────────────────────────────────
PICKUPS  = [(-1.9490, 30.0900), (-1.9443, 30.0619), (-1.9590, 30.0960)]
DROPOFFS = [(-1.9358, 30.1069), (-1.9363, 30.1300), (-1.9700, 30.1180)]
CATS     = ["DOCUMENTS", "GENERAL", "PERISHABLE"]

FRAME_SEC = 3.0          # telemetry cadence (design spec)
ALERT_TIMEOUT = 25.0     # wait for DISPATCH before going idle
OP_TIMEOUT = 20          # per-HTTP-op timeout


class Metrics:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.lat_e2e = []        # create -> delivered
        self.lat_dispatch = []   # create -> accepted
        self.lat_create = []
        self.lat_accept = []
        self.lat_pickup = []
        self.lat_deliver = []
        self.counters = {}

    async def add(self, bucket, ms):
        async with self.lock:
            {"e2e": self.lat_e2e, "dispatch": self.lat_dispatch,
             "create": self.lat_create, "accept": self.lat_accept,
             "pickup": self.lat_pickup, "deliver": self.lat_deliver}[bucket].append(ms)

    async def inc(self, key, n=1):
        async with self.lock:
            self.counters[key] = self.counters.get(key, 0) + n

    def report(self, orders_created):
        def pct(xs, p):
            if not xs:
                return 0
            xs = sorted(xs)
            k = max(0, min(len(xs) - 1, int(round(p / 100 * len(xs) + 0.5)) - 1))
            return xs[k]
        c = self.counters
        delivered = len(self.lat_e2e)
        print("\n════════ STRESS REPORT ════════")
        print(f"  users(provisioned)      : {c.get('drivers_up', 0)} drivers, "
              f"{c.get('customers_up', 0)} customers")
        print(f"  orders created          : {orders_created}")
        print(f"  orders accepted         : {c.get('accepted', 0)}")
        print(f"  orders delivered        : {delivered}")
        print(f"  order failures          : create={c.get('create_fail', 0)} "
              f"accept={c.get('accept_fail', 0)} pickup={c.get('pickup_fail', 0)} "
              f"deliver={c.get('deliver_fail', 0)} lock={c.get('ORDER_LOCKED', 0)}")
        print(f"  websocket               : errors={c.get('ws_err', 0)} "
              f"reconnects={c.get('ws_reconn', 0)} frames_sent={c.get('frames', 0)}")
        if self.lat_e2e:
            e = self.lat_e2e
            print(f"  end-to-end (create→deliver) s: p50={pct(e,50)/1000:.1f} "
                  f"p95={pct(e,95)/1000:.1f} p99={pct(e,99)/1000:.1f} "
                  f"max={max(e)/1000:.1f} mean={statistics.mean(e)/1000:.1f}")
        if self.lat_dispatch:
            d = self.lat_dispatch
            print(f"  dispatch wait (create→accept) s: p50={pct(d,50)/1000:.1f} "
                  f"p95={pct(d,95)/1000:.1f} max={max(d)/1000:.1f}")
        for name, xs in (("create", self.lat_create), ("accept", self.lat_accept),
                         ("pickup", self.lat_pickup), ("deliver", self.lat_deliver)):
            if xs:
                print(f"  op {name:7s} ms: p50={pct(xs,50):.0f} p95={pct(xs,95):.0f} "
                      f"p99={pct(xs,99):.0f} max={max(xs):.0f}")


M = Metrics()
ARGS = None
STOP = asyncio.Event()
BASE = "http://localhost:8080"


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def gql(session, token, op, variables):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with session.post(
        f"{BASE}/graphql",
        json={"query": "m", "operationName": op, "variables": variables},
        headers=headers, timeout=aiohttp.ClientTimeout(total=OP_TIMEOUT),
    ) as r:
        return await r.json(content_type=None)


async def auth_register(session, name, phone, role):
    async with session.post(
        f"{BASE}/auth/register",
        json={"query": "m", "operationName": "Register",
              "variables": {"input": {"fullName": name, "phone": phone,
                                      "password": "StressPassw0rd!", "role": role}}},
        timeout=aiohttp.ClientTimeout(total=OP_TIMEOUT),
    ) as r:
        body = await r.json(content_type=None)
    if body.get("errors"):
        # phone collision (fast re-run) → login instead
        async with session.post(
            f"{BASE}/auth/login",
            json={"query": "m", "operationName": "Login",
                  "variables": {"input": {"phone": phone,
                                          "password": "StressPassw0rd!"}}},
            timeout=aiohttp.ClientTimeout(total=OP_TIMEOUT),
        ) as r:
            body = await r.json(content_type=None)
    d = body["data"]
    return d["token"], d["user"]["id"]


def phone_with(prefix, salt, i):
    return f"{prefix}{(salt + i * 7919) % 100_000_000:08d}"  # 7919: spreads digits


# ── driver worker ─────────────────────────────────────────────────────────────

async def driver_worker(idx, session, salt, online_evt):
    """One virtual driver: register, stream telemetry, fulfill orders until STOP."""
    run_tag = uuid.uuid4().hex[:6].upper()
    try:
        token = uid = None
        for attempt in range(3):
            try:
                token, uid = await auth_register(
                    session, f"Stress Driver {idx}",
                    phone_with("+25070", salt, idx), "DRIVER")
                break
            except Exception as e:
                if attempt == 2:
                    raise
                await M.inc("reg_retry")
                if ARGS.debug:
                    log(f"driver{idx}: register attempt {attempt+1} failed: {e!r}")
                await asyncio.sleep(2 * (attempt + 1))
    except Exception as e:
        await M.inc("reg_fail")
        log(f"driver{idx}: register failed: {e!r}")
        return

    # Profile + admin approval (RBAC: self-approval is blocked) + online.
    try:
        await gql(session, token, "RegisterDriver", {"input": {
            "nationalId": f"1199{idx % 100000:05d}{run_tag[:5]}",
            "licenseNumber": f"LIC-{run_tag}-{idx:05d}",
            "vehicleType": "MOTORCYCLE", "plateNumber": f"ST{run_tag[:4]}{idx % 100}",
            "maxWeightKg": 30}})
        await gql(ADMIN_SESSION, ADMIN_TOKEN, "ApproveDriver", {"driverUserId": uid})
        await gql(session, token, "SetDriverOnline", {})
    except Exception:
        pass  # non-fatal: keep trying to stream; acceptance will just fail later
    await M.inc("drivers_up")
    online_evt.set()

    ws_url = BASE.replace("http", "ws", 1) + "/ws/telemetry"
    lat, lng = random.choice(PICKUPS)
    lat += random.uniform(-0.004, 0.004)
    lng += random.uniform(-0.004, 0.004)
    seq = 0
    dispatched = asyncio.Queue()

    while not STOP.is_set():
        # ── WS session (reconnect loop) ──
        try:
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {token}"},
                ping_interval=20, ping_timeout=20, max_size=1 << 20,
            ) as ws:
                receiver = asyncio.create_task(ws_receive(ws, dispatched))
                # Telemetry + fulfillment while the socket is healthy
                stream = asyncio.create_task(
                    ws_stream(ws, token, uid, lat, lng, dispatched, seq))
                await stream
                receiver.cancel()
                lat, lng = stream.result() if not stream.cancelled() else (lat, lng)
        except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as e:
            await M.inc("ws_err")
            if STOP.is_set():
                return
            await M.inc("ws_reconn")
            await asyncio.sleep(min(2 ** random.random(), 8))
        except Exception as e:
            await M.inc("ws_fatal")
            log(f"driver{idx}: ws fatal: {type(e).__name__}: {e}")
            return


async def ws_receive(ws, dispatched):
    """Pump inbound messages; DISPATCH alerts enqueue delivery ids."""
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if msg.get("type") == "ALERT":
                try:
                    alert = json.loads(msg.get("payload", "{}"))
                except ValueError:
                    continue
                # AnomalyAlert json tags are snake_case (domain/telemetry.go).
                if alert.get("alert_type") == "DISPATCH" and alert.get("delivery_id"):
                    await dispatched.put(alert["delivery_id"])
    except (websockets.WebSocketException, asyncio.CancelledError):
        pass


async def ws_stream(ws, token, uid, lat, lng, dispatched, seq):
    """Send a FRAME every FRAME_SEC; between frames, fulfill dispatched orders."""
    next_frame = time.monotonic()
    try:
        while not STOP.is_set():
            # Fulfillment step (non-blocking wait for a dispatch)
            try:
                delivery_id = await asyncio.wait_for(dispatched.get(),
                                                     timeout=FRAME_SEC)
                await fulfill(session_from(token), token, uid, delivery_id)
            except asyncio.TimeoutError:
                pass
            if STOP.is_set():
                return (lat, lng)
            # Telemetry frame
            seq += 1
            lat += 0.00015
            lng += 0.00015
            frame = json.dumps({
                "type": "FRAME", "delivery_id": "", "lat": round(lat, 6),
                "lng": round(lng, 6), "speed_kmh": 28.0, "bearing": 135.0,
                "accuracy_m": 5.0, "battery_pct": 80.0,
                "ts_ms": int(time.time() * 1000), "seq": seq})
            await ws.send(frame)
            await M.inc("frames")
            next_frame += FRAME_SEC
            await asyncio.sleep(max(0.0, next_frame - time.monotonic()))
    except (websockets.WebSocketException, OSError):
        raise  # bubbles to the reconnect loop
    return (lat, lng)


# Session is passed via contextvar-free trick: aiohttp session is global (one pool)
SESSION = None

# Admin JWT for RBAC-gated ops (ApproveDriver). Minted in main() via
# scripts/admin_token.py — privileged roles cannot self-register.
ADMIN_TOKEN = None
ADMIN_SESSION = None

# delivery_id -> (plaintextQrCode, plaintextPin), written by the merchant blast
# at CreateDelivery and consumed by the winning driver at ConfirmPickup.
QRBOOK = {}


def session_from(_token):
    return SESSION


async def fulfill(session, token, uid, delivery_id):
    """accept -> pickup -> deliver one order; records all latencies.

    The plaintext QR/PIN are returned exactly once by CreateDelivery (merchant
    side), so the merchant blast shares them via QRBOOK and the fulfilling
    driver reads them from there — mirroring the real custody handoff.
    """
    t0 = time.monotonic()
    tokens = QRBOOK.get(delivery_id)
    if tokens is None:
        await M.inc("no_qr_tokens")
        return
    qr, pin = tokens
    try:
        body = await gql(session, token, "AcceptOrder", {"deliveryId": delivery_id})
        t1 = time.monotonic()
        if body.get("errors"):
            code = body["errors"][0].get("extensions", {}).get("code", "?")
            await M.inc(code)
            await M.inc("accept_fail")
            return
        await M.inc("accepted")
        await M.add("dispatch", (t1 - t0) * 1000)
        await M.add("accept", (t1 - t0) * 1000)

        t0p = time.monotonic()
        body = await gql(session, token, "ConfirmPickup",
                         {"deliveryId": delivery_id, "scannedToken": qr,
                          "confirmedWeightKg": 0})
        t1p = time.monotonic()
        if body.get("errors"):
            await M.inc("pickup_fail")
            return
        await M.add("pickup", (t1p - t0p) * 1000)

        t0d = time.monotonic()
        body = await gql(session, token, "ConfirmDelivery",
                         {"deliveryId": delivery_id, "customerPin": pin})
        t1d = time.monotonic()
        if body.get("errors"):
            await M.inc("deliver_fail")
            return
        await M.add("deliver", (t1d - t0d) * 1000)
        await M.add("e2e", (t1d - t0) * 1000)
    except Exception as e:
        await M.inc("fulfill_err")
        if ARGS.debug:
            log(f"fulfill error: {type(e).__name__}: {e}")


# ── merchant order blast ─────────────────────────────────────────────────────

async def order_blast(merchant_token, customer_ids, order_counter, creator_idx):
    # Wait for the ramp gate (set in main once a meaningful fraction of the
    # fleet is streaming) — an empty geo index would starve dispatch.
    await ONLINE_THRESHOLD.wait()
    # Parallel creators interleave: each sleeps creators×interval so the fleet
    # produces the requested aggregate rate without one 1.6s create capping it.
    await asyncio.sleep(creator_idx * ARGS.order_interval)
    n = 0
    while not STOP.is_set() and (ARGS.orders == 0 or n < ARGS.orders):
        pickup = random.choice(PICKUPS)
        dropoff = random.choice(DROPOFFS)
        cid = random.choice(customer_ids)
        t0 = time.monotonic()
        try:
            body = await gql(SESSION, merchant_token, "CreateDelivery", {"input": {
                "customerId": cid,
                "pickupLocation": {"lat": pickup[0], "lng": pickup[1]},
                "dropoffLocation": {"lat": dropoff[0], "lng": dropoff[1]},
                "description": f"stress {n}", "weightKg": 1.0,
                "packageCategory": random.choice(CATS),
                "isPrepaid": True, "prepTimeMinutes": random.choice([1, 2, 3]),
            }})
        except Exception:
            await M.inc("create_fail")
            await asyncio.sleep(ARGS.order_interval)
            continue
        if body.get("errors"):
            await M.inc("create_fail")
            if ARGS.debug:
                log(f"create failed: {body['errors'][0]['message'][:100]}")
        else:
            d = body["data"]["delivery"]
            QRBOOK[d["id"]] = (body["data"]["plaintextQrCode"],
                               body["data"]["plaintextPin"])
            n += 1
            order_counter[0] += 1  # aggregate across parallel creators
            await M.add("create", (time.monotonic() - t0) * 1000)
        await asyncio.sleep(ARGS.order_interval * ARGS.creators)
    STOP.set()


async def resource_sampler(path="stress_sampler.log"):
    """Sample container CPU/RAM every 15 s into a file for capacity planning."""
    fmt = "{{.Name}} CPU={{.CPUPerc}} MEM={{.MemUsage}}"
    with open(path, "a") as f:
        while not STOP.is_set():
            try:
                proc = await asyncio.create_subprocess_exec(
                    "docker", "stats", "--no-stream", "--format", fmt,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL)
                out, _ = await asyncio.wait_for(proc.communicate(), timeout=12)
                f.write(f"=== {time.strftime('%H:%M:%S')} ===\n")
                f.write(out.decode(errors="replace"))
                f.flush()
            except Exception:
                pass
            await asyncio.sleep(15)


ONLINE_THRESHOLD = asyncio.Event()


async def progress_watchdog(order_counter, orders_target, deadline):
    while not STOP.is_set():
        if time.monotonic() > deadline:
            log("deadline reached — stopping")
            STOP.set()
            return
        if orders_target and M.counters.get("accepted", 0) >= orders_target:
            log("order target reached — stopping")
            STOP.set()
            return
        await asyncio.sleep(2)


def fmt_counter(key):
    return M.counters.get(key, 0)


async def heartbeat(order_counter):
    """One-line status every 15 s so long runs aren't a black hole."""
    while not STOP.is_set():
        await asyncio.sleep(15)
        log(f"  … drivers_up={fmt_counter('drivers_up')} "
            f"orders={order_counter[0]} accepted={fmt_counter('accepted')} "
            f"delivered={len(M.lat_e2e)} ws_err={fmt_counter('ws_err')} "
            f"frames={fmt_counter('frames')} reg_fail={fmt_counter('reg_fail')}")


async def main():
    global ARGS, BASE, SESSION, ADMIN_TOKEN, ADMIN_SESSION
    ap = argparse.ArgumentParser(description="Umurinzi async stress harness")
    ap.add_argument("--base", default="http://localhost:8080")
    ap.add_argument("--users", type=int, default=500, help="number of virtual drivers")
    ap.add_argument("--duration", type=int, default=120, help="run seconds after ramp")
    ap.add_argument("--orders", type=int, default=0,
                    help="order cap (0 = continuous blast at --order-interval)")
    ap.add_argument("--order-interval", type=float, default=1.0,
                    help="seconds between order creations")
    ap.add_argument("--customers", type=int, default=0,
                    help="customer accounts (default: users//5, capped 500)")
    ap.add_argument("--batch", type=int, default=100,
                    help="registration wave size (pacing is adaptive)")
    ap.add_argument("--creators", type=int, default=4,
                    help="parallel merchant creators (aggregate rate = creators/interval)")
    ap.add_argument("--debug", action="store_true")
    ARGS = ap.parse_args()
    BASE = ARGS.base.rstrip("/")

    salt = int(time.time()) % 100_000_000
    n_customers = ARGS.customers or min(max(ARGS.users // 5, 10), 500)

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        SESSION = session

        # RBAC: ApproveDriver requires ADMIN — mint the harness admin JWT.
        import importlib.util
        _spec = importlib.util.spec_from_file_location(
            "admin_token", os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts", "admin_token.py"))
        _at = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_at)
        ADMIN_TOKEN, ADMIN_SESSION = _at.mint_admin_token(), session

        log(f"provisioning merchant + {n_customers} customers …")
        m_token, _ = await auth_register(session, "Stress Merchant",
                                         phone_with("+25078", salt, 999_999), "MERCHANT")
        # Orders are escrow-funded since the wallet layer went live — the
        # blast merchant needs a balance to sustain the run. TopUp caps at
        # MaxTopUpRWF (2,000,000), so top up to the cap.
        try:
            body = await gql(session, m_token, "TopUpWallet", {"amountRwf": 2_000_000})
            if body.get("errors"):
                log(f"merchant topup FAILED: {body['errors'][0]['message'][:80]}")
            else:
                log(f"merchant wallet funded: {body['data']['balanceRwf']} RWF")
        except Exception as e:
            log(f"merchant topup failed ({e!r}) — order creates will be rejected")
        async def one_customer(i):
            try:
                _, uid = await auth_register(
                    session, f"Stress Cust {i}", phone_with("+25072", salt, i), "CUSTOMER")
                await M.inc("customers_up")
                return uid
            except Exception:
                await M.inc("reg_fail")
                return None
        customer_ids = [c for c in await gather_chunked(
            one_customer, range(n_customers), ARGS.batch) if c]
        log(f"{len(customer_ids)} customers ready — spawning {ARGS.users} drivers")

        # Launch the fleet in staggered chunks. create_task (not awaited gather):
        # workers run until STOP, so awaiting a chunk would block the next one.
        # The 50 ms inter-chunk sleep throttles the registration/WS storm.
        async def launch_fleet(n, batch, salt, evt):
            tasks = []
            prev_up = 0
            for start in range(0, n, batch):
                for i in range(start, min(start + batch, n)):
                    tasks.append(asyncio.create_task(
                        driver_worker(i, session, salt, evt)))
                # Adaptive pacing: each wave of registrations burns real CPU
                # server-side (bcrypt cost 12 ≈ 250ms/request). Launching the
                # whole fleet at once makes every register time out.
                target = prev_up + max(1, int(batch * 0.5))
                t0 = time.monotonic()
                while (M.counters.get("drivers_up", 0) < target
                       and time.monotonic() - t0 < 30):
                    await asyncio.sleep(0.5)
                prev_up = M.counters.get("drivers_up", 0)
                log(f"  ramp: {prev_up}/{n} drivers online")
            return tasks

        online_evt = asyncio.Event()
        sampler = asyncio.create_task(resource_sampler())
        ramp_task = asyncio.create_task(
            launch_fleet(ARGS.users, ARGS.batch, salt, online_evt))

        log("waiting for first drivers to come online …")
        try:
            await asyncio.wait_for(online_evt.wait(), timeout=60)
        except asyncio.TimeoutError:
            log("ramp timeout — blasting anyway")
        await asyncio.sleep(10)  # let a meaningful fraction of the fleet stream
        ONLINE_THRESHOLD.set()

        deadline = time.monotonic() + ARGS.duration
        order_counter = [0]
        if ARGS.creators > 0:
            creators = [asyncio.create_task(
                order_blast(m_token, customer_ids, order_counter, i))
                for i in range(ARGS.creators)]
            blast = asyncio.gather(*creators)
        else:
            # Pure connection/telemetry load: no orders. The watchdog owns STOP.
            log("--creators 0: telemetry-only run (no orders will be created)")
            blast = progress_watchdog(order_counter, 0, deadline)
        watch = asyncio.create_task(progress_watchdog(order_counter, ARGS.orders, deadline))
        beat = asyncio.create_task(heartbeat(order_counter))

        await blast
        STOP.set()
        sampler.cancel()
        watch.cancel()
        beat.cancel()
        # Give worker coroutines a few seconds to notice STOP and close their
        # sockets cleanly before the loop tears down pending tasks.
        await asyncio.sleep(4)
        ramp_task.cancel()
        try:
            await ramp_task
        except (asyncio.CancelledError, Exception):
            pass

        M.report(order_counter[0])


async def ramp_first_online(ramp_task, evt):
    await ramp_task


async def gather_chunked(fn, iterable, chunk):
    it = list(iterable)
    out = []
    for i in range(0, len(it), chunk):
        results = await asyncio.gather(*[fn(x) for x in it[i:i + chunk]],
                                       return_exceptions=True)
        out += [r for r in results if not isinstance(r, BaseException)]
        await asyncio.sleep(0.05)
    return out


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        STOP.set()
        M.report(0)
