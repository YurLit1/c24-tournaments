import os
import random
import time

import httpx
import ray

RECEIVER_URL = os.getenv("RECEIVER_URL", "http://api:8000")
API_TOKEN = os.getenv("API_TOKEN", "dev-token")
TASK_RETRIES = int(os.getenv("TASK_RETRIES", "3"))


def verify(match_id: str) -> bool:
    rnd = random.Random(match_id)
    time.sleep(rnd.uniform(0.1, 0.5))
    return rnd.random() > 0.05


@ray.remote(max_retries=TASK_RETRIES, retry_exceptions=False)
def process_match(request_id: str, match_id: str, winner_id: str, loser_id: str, season: str) -> dict:
    if not verify(match_id):
        return {"verified": False, "pid": os.getpid()}
    effect = {"request_id": request_id, "match_id": match_id, "winner_id": winner_id,
              "loser_id": loser_id, "season": season}
    reply = httpx.post(f"{RECEIVER_URL}/internal/effects", json=effect,
                       headers={"Authorization": f"Bearer {API_TOKEN}"}, timeout=30)
    reply.raise_for_status()
    decision = reply.json()
    return {"verified": True, "decision": decision, "pid": os.getpid()}
