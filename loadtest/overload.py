import argparse
import asyncio
import multiprocessing as mp
import os
import platform
import random
import statistics
import time
import uuid
from collections import Counter

import httpx

PLAYERS = [f"00000000-0000-0000-0000-{i:012d}" for i in range(1, 11)]


def probe(base, duration, interval, out):
    samples = []
    max_queue = 0
    with httpx.Client(base_url=base, timeout=5) as c:
        end = time.monotonic() + duration
        while time.monotonic() < end:
            t0 = time.perf_counter()
            r = c.get("/health")
            samples.append((time.perf_counter() - t0) * 1000)
            max_queue = max(max_queue, r.json().get("queue_size", 0))
            time.sleep(interval)
    out.put((samples, max_queue))


async def flood(base, token, clients, duration):
    counts = Counter()
    end = time.monotonic() + duration
    headers = {"Authorization": f"Bearer {token}"}
    limits = httpx.Limits(max_connections=clients)

    async def one(c):
        while time.monotonic() < end:
            winner, loser = random.sample(PLAYERS, 2)
            body = {"match_id": str(uuid.uuid4()), "winner_id": winner, "loser_id": loser}
            try:
                r = await c.post("/v1/match-results", json=body)
                counts[r.status_code] += 1
            except httpx.HTTPError:
                counts["error"] += 1

    async with httpx.AsyncClient(base_url=base, headers=headers, timeout=10, limits=limits) as c:
        cfg = (await c.get("/health")).json()
        await asyncio.gather(*(one(c) for _ in range(clients)))
    return counts, cfg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8090")
    ap.add_argument("--token", default="dev-token")
    ap.add_argument("--clients", type=int, default=100)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--interval", type=float, default=0.05)
    a = ap.parse_args()

    out = mp.Queue()
    p = mp.Process(target=probe, args=(a.base, a.duration - 4, a.interval, out))

    async def run():
        task = asyncio.create_task(flood(a.base, a.token, a.clients, a.duration))
        await asyncio.sleep(2)
        p.start()
        return await task

    counts, cfg = asyncio.run(run())
    samples, max_queue = out.get()
    p.join()

    q = statistics.quantiles(samples, n=100)
    print(f"machine: {platform.processor() or platform.machine()}, logical CPUs: {os.cpu_count()}, "
          f"python {platform.python_version()}")
    print(f"service: workers={cfg['workers']} queue_capacity={cfg['queue_capacity']} rating_sync={cfg['rating_sync']}")
    print(f"load: {a.clients} concurrent clients, {a.duration} s, no pauses")
    print(f"responses: 202={counts.get(202, 0)} 429={counts.get(429, 0)} other={sum(v for k, v in counts.items() if k not in (202, 429))}")
    print(f"max queue_size seen by probe: {max_queue} / {cfg['queue_capacity']}")
    print(f"/health latency, {len(samples)} samples every {int(a.interval * 1000)} ms: "
          f"p50={q[49]:.1f} ms  p95={q[94]:.1f} ms  p99={q[98]:.1f} ms  max={max(samples):.1f} ms")
    print("p95 <= 100 ms: " + ("YES" if q[94] <= 100 else "NO"))


if __name__ == "__main__":
    main()
