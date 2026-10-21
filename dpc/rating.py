import asyncio
import os
import uuid

import asyncpg

RATING_SYNC = os.getenv("RATING_SYNC", "lock")
START_RATING = 1500
RACE_WINDOW_S = 0.02


def elo_delta(r_win: int, r_lose: int, k: int = 32) -> int:
    expected = 1 / (1 + 10 ** ((r_lose - r_win) / 400))
    return max(1, round(k * (1 - expected)))


async def apply_rating(pool: asyncpg.Pool, request_id: uuid.UUID, match_id: uuid.UUID,
                       winner_id: uuid.UUID, loser_id: uuid.UUID) -> bool:
    lock = "FOR UPDATE" if RATING_SYNC == "lock" else ""
    async with pool.acquire() as conn:
        async with conn.transaction():
            first = await conn.fetchval(
                "INSERT INTO match_result_applied (match_id, request_id) VALUES ($1, $2) "
                "ON CONFLICT DO NOTHING RETURNING 1", match_id, request_id)
            if not first:
                return False
            rows = await conn.fetch(
                f"SELECT player_id, rating, version FROM player_rating "
                f"WHERE player_id = ANY($1::uuid[]) ORDER BY player_id {lock}", [winner_id, loser_id])
            r = {row["player_id"]: row for row in rows}
            if RATING_SYNC == "none":
                await asyncio.sleep(RACE_WINDOW_S)
            d = elo_delta(r[winner_id]["rating"], r[loser_id]["rating"])
            for pid, delta in sorted(((winner_id, d), (loser_id, -d)), key=lambda x: x[0]):
                await conn.execute(
                    "UPDATE player_rating SET rating = $2, version = $3 WHERE player_id = $1",
                    pid, r[pid]["rating"] + delta, r[pid]["version"] + 1)
    return True


async def rating_state(pool: asyncpg.Pool, player_id: uuid.UUID) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT p.player_id, p.rating, p.version, "
        "  (SELECT sum(rating) FROM player_rating) = $2 * (SELECT count(*) FROM player_rating) "
        "  AND (SELECT sum(version) FROM player_rating) = 2 * (SELECT count(*) FROM match_result_applied) "
        "  AS invariant_ok "
        "FROM player_rating p WHERE p.player_id = $1", player_id, START_RATING)
