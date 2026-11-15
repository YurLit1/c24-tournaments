import argparse
import subprocess
import time
import uuid

import httpx

WINNER = "00000000-0000-0000-0000-000000000007"
LOSER = "00000000-0000-0000-0000-000000000008"
FINAL = {"done", "rejected", "failed"}


def compose(*args: str) -> str:
    return subprocess.run(["docker", "compose", *args], capture_output=True, text=True, encoding="utf-8").stdout


def ray_fault_hits() -> int:
    total = 0
    for service in ("ray-head", "ray-worker"):
        out = compose("exec", "-T", service, "sh", "-c",
                      "cat /tmp/ray/session_latest/logs/worker-*.out 2>/dev/null || true")
        total += out.count('"fault.hit"')
    return total


def deliveries(match_id: str) -> list[str]:
    return [line for line in compose("logs", "api").splitlines()
            if '"effect.received"' in line and match_id in line]


def sql(query: str) -> str:
    return compose("exec", "-T", "postgres", "psql", "-At", "-U", "app", "-d", "tournaments", "-c", query).strip()


def wait_final(c: httpx.Client, location: str) -> dict:
    for _ in range(240):
        state = c.get(location).json()
        if state["state"] in FINAL:
            return state
        time.sleep(0.5)
    raise TimeoutError("request did not reach a final state")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8090")
    ap.add_argument("--token", default="dev-token")
    a = ap.parse_args()

    with httpx.Client(base_url=a.base, headers={"Authorization": f"Bearer {a.token}"}, timeout=30) as c:
        health = c.get("/health").json()
        print(f"execution={health['execution']} ray_connected={health['ray_connected']}")
        season = c.get("/v1/season-awards").json()["season"]
        hits_before = ray_fault_hits()

        while True:
            match_id = str(uuid.uuid4())
            compose("exec", "-T", "api", "touch", "/faults/after_effect_before_ack")
            started = time.perf_counter()
            r = c.post("/v1/match-results", json={"match_id": match_id, "winner_id": WINNER, "loser_id": LOSER})
            r.raise_for_status()
            state = wait_final(c, r.headers["location"])
            if state["state"] == "rejected" and state["reason"].startswith("game server"):
                compose("exec", "-T", "api", "rm", "-f", "/faults/after_effect_before_ack")
                print(f"match {match_id} was not confirmed by the game server, trying another match")
                continue
            break

        elapsed = time.perf_counter() - started
        hits_after = ray_fault_hits()
        armed = compose("exec", "-T", "api", "ls", "/faults")
        applied = sql(f"SELECT count(*) FROM match_result_applied WHERE match_id = '{match_id}'")
        awards = sql(f"SELECT count(*) FROM season_award WHERE player_id = '{WINNER}' AND season = '{season}'")
        invariant = c.get(f"/v1/player-rating/{WINNER}").json()["invariant_ok"]
        delivered = deliveries(match_id)

    print(f"match_id={match_id}")
    print(f"final state: {state['state']} after {elapsed:.1f} s")
    print(f"fault.hit lines in Ray worker logs: {hits_before} -> {hits_after}; flag file still armed: {'after_effect_before_ack' in armed}")
    print(f"effect deliveries to the receiver: {len(delivered)}")
    for line in delivered:
        print("   ", line.split("|", 1)[-1].strip())
    print(f"rating effects for this match (match_result_applied): {applied}")
    print(f"season awards for ({WINNER}, {season}): {awards}")
    print(f"invariant_ok: {invariant}")
    repeated = len(delivered) == 2 and '"repeat": true' in delivered[-1]
    ok = (state["state"] == "done" and hits_after == hits_before + 1 and repeated
          and applied == "1" and awards == "1" and invariant)
    print("ONE EFFECT AFTER RETRY, INVARIANT HOLDS" if ok else "CHECK FAILED")


if __name__ == "__main__":
    main()
