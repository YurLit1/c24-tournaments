import argparse
import asyncio
import random
import uuid
from collections import Counter

import httpx

PLAYERS = [f"00000000-0000-0000-0000-{i:012d}" for i in range(1, 11)]
FINAL = {"done", "rejected", "failed"}


async def submit(client, sem, hot):
    winner, loser = random.sample(hot, 2)
    body = {"match_id": str(uuid.uuid4()), "winner_id": winner, "loser_id": loser}
    async with sem:
        while True:
            r = await client.post("/v1/match-results", json=body)
            if r.status_code == 202:
                return r.json()["id"]
            if r.status_code == 429:
                await asyncio.sleep(float(r.headers.get("Retry-After", "1")))
                continue
            raise RuntimeError(f"unexpected {r.status_code}: {r.text}")


async def status_of(client, sem, req_id):
    async with sem:
        r = await client.get(f"/v1/match-results/{req_id}")
        r.raise_for_status()
        return r.json()["state"]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8091")
    ap.add_argument("--token", default="dev-token")
    ap.add_argument("--n", type=int, default=500)
    ap.add_argument("--hot", type=int, default=3)
    ap.add_argument("--concurrency", type=int, default=50)
    a = ap.parse_args()

    hot = PLAYERS[:a.hot]
    headers = {"Authorization": f"Bearer {a.token}"}
    limits = httpx.Limits(max_connections=a.concurrency)
    async with httpx.AsyncClient(base_url=a.base, headers=headers, timeout=30, limits=limits) as c:
        health = (await c.get("/health")).json()
        print(f"mode={health['rating_sync']} workers={health['workers']} "
              f"queue_capacity={health['queue_capacity']} n={a.n} hot_players={a.hot}")

        sem = asyncio.Semaphore(a.concurrency)
        ids = await asyncio.gather(*(submit(c, sem, hot) for _ in range(a.n)))
        print(f"accepted (202): {len(ids)}")

        pending = list(ids)
        statuses = {}
        while pending:
            got = await asyncio.gather(*(status_of(c, sem, i) for i in pending))
            for i, s in zip(pending, got):
                statuses[i] = s
            pending = [i for i in pending if statuses[i] not in FINAL]
            if pending:
                await asyncio.sleep(0.5)

        counts = Counter(statuses.values())
        ratings = await asyncio.gather(*(c.get(f"/v1/player-rating/{p}") for p in PLAYERS))
        rows = [r.json() for r in ratings]

    sum_rating = sum(r["rating"] for r in rows)
    sum_version = sum(r["version"] for r in rows)
    done = counts.get("done", 0)
    print(f"statuses: {dict(counts)}")
    print(f"sum(rating)  = {sum_rating}  (expected {1500 * len(PLAYERS)})")
    print(f"sum(version) = {sum_version}  (expected 2 x done = {2 * done})")
    print(f"invariant_ok from GET /v1/player-rating: {rows[0]['invariant_ok']}")
    ok = sum_rating == 1500 * len(PLAYERS) and sum_version == 2 * done
    print("INVARIANT HOLDS" if ok else "INVARIANT VIOLATED: lost updates")


if __name__ == "__main__":
    asyncio.run(main())
