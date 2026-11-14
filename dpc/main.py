import asyncio
import json
import os
import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from typing import Literal

from pydantic import BaseModel, Field, model_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

import compute
import rating

QUEUE_CAPACITY = int(os.getenv("QUEUE_CAPACITY", "50"))
WORKERS = int(os.getenv("WORKERS", "4"))
API_TOKEN = os.getenv("API_TOKEN", "dev-token")
RETRY_AFTER_S = 1


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=WORKERS + 10)
    app.state.queue = asyncio.Queue(maxsize=QUEUE_CAPACITY)
    app.state.jobs = {}
    app.state.workers = [asyncio.create_task(worker()) for _ in range(WORKERS)]
    yield
    for w in app.state.workers:
        w.cancel()
    await asyncio.gather(*app.state.workers, return_exceptions=True)
    await app.state.pool.close()


app = FastAPI(title="C24 Tournaments — DPC", lifespan=lifespan)


class ProblemError(Exception):
    def __init__(self, status: int, title: str, detail: str, headers: dict | None = None):
        self.status, self.title, self.detail, self.headers = status, title, detail, headers


def problem(request: Request, status: int, title: str, detail: str, headers: dict | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        media_type="application/problem+json",
        headers=headers,
        content={"type": "about:blank", "title": title, "status": status,
                 "detail": detail, "instance": request.url.path},
    )


@app.exception_handler(ProblemError)
async def on_problem(request: Request, exc: ProblemError):
    return problem(request, exc.status, exc.title, exc.detail, exc.headers)


@app.exception_handler(RequestValidationError)
async def on_validation(request: Request, exc: RequestValidationError):
    if any(e["type"] == "json_invalid" for e in exc.errors()):
        return problem(request, 400, "Malformed JSON", "Request body is not valid JSON")
    msg = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
    return problem(request, 422, "Invalid data", msg)


@app.exception_handler(StarletteHTTPException)
async def on_http(request: Request, exc: StarletteHTTPException):
    return problem(request, exc.status_code, str(exc.detail), str(exc.detail))


@app.exception_handler(Exception)
async def on_unexpected(request: Request, exc: Exception):
    print(json.dumps({"time": datetime.now(timezone.utc).isoformat(), "event": "request.crash",
                      "path": request.url.path, "error": type(exc).__name__, "message": str(exc)[:300]}), flush=True)
    return problem(request, 500, "Internal Server Error", "Unexpected error")


bearer = HTTPBearer(auto_error=False)


def auth(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
    if credentials is None or credentials.credentials != API_TOKEN:
        raise ProblemError(401, "Unauthorized", "Missing or invalid bearer token",
                           {"WWW-Authenticate": "Bearer"})


def to_json(row: asyncpg.Record) -> dict:
    out = {}
    for k, v in dict(row).items():
        if isinstance(v, uuid.UUID):
            v = str(v)
        elif isinstance(v, datetime):
            v = v.isoformat()
        out[k] = v
    return out


class MatchResultIn(BaseModel):
    match_id: uuid.UUID
    winner_id: uuid.UUID
    loser_id: uuid.UUID

    @model_validator(mode="after")
    def different_players(self):
        if self.winner_id == self.loser_id:
            raise ValueError("winner_id and loser_id must differ")
        return self


def queue_full() -> ProblemError:
    return ProblemError(429, "Queue is full", f"Queue capacity {QUEUE_CAPACITY} reached, retry later",
                        {"Retry-After": str(RETRY_AFTER_S)})


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "queue_size": app.state.queue.qsize(),
        "queue_capacity": QUEUE_CAPACITY,
        "workers": sum(not w.done() for w in app.state.workers),
        "rating_sync": rating.RATING_SYNC,
    }


@app.post("/v1/match-results", status_code=202, dependencies=[Depends(auth)])
async def accept_match_result(body: MatchResultIn):
    q = app.state.queue
    if q.full():
        raise queue_full()
    req_id = uuid.uuid4()
    try:
        await app.state.pool.execute(
            "INSERT INTO match_result_request (id, match_id, winner_id, loser_id, state) "
            "VALUES ($1, $2, $3, $4, 'queued')",
            req_id, body.match_id, body.winner_id, body.loser_id)
    except asyncpg.ForeignKeyViolationError:
        raise ProblemError(422, "Unknown player", "winner_id or loser_id is not a rated player")
    try:
        q.put_nowait((req_id, body))
    except asyncio.QueueFull:
        await app.state.pool.execute("DELETE FROM match_result_request WHERE id = $1", req_id)
        raise queue_full()
    return JSONResponse(status_code=202, content={"id": str(req_id), "state": "queued"},
                        headers={"Location": f"/v1/match-results/{req_id}"})


@app.get("/v1/match-results/{request_id}", dependencies=[Depends(auth)])
async def get_match_result(request_id: uuid.UUID):
    row = await app.state.pool.fetchrow(
        "SELECT id, match_id, winner_id, loser_id, state, reason, created_at, updated_at "
        "FROM match_result_request WHERE id = $1", request_id)
    if row is None:
        raise ProblemError(404, "Request not found", f"No match result request {request_id}")
    return to_json(row)


@app.get("/v1/player-rating/{player_id}", dependencies=[Depends(auth)])
async def get_player_rating(player_id: uuid.UUID):
    row = await rating.rating_state(app.state.pool, player_id)
    if row is None:
        raise ProblemError(404, "Player not found", f"No rating for player {player_id}")
    return to_json(row)


class ComputeIn(BaseModel):
    a: float = 0.0
    b: float = 168.0
    n: int = Field(default=1_000_000, ge=2, le=200_000_000)
    workers: int = Field(default=1, ge=1, le=16)
    method: Literal["midpoint", "trapezoid", "simpson"] = "midpoint"
    order: Literal["forward", "reverse"] = "forward"
    mode: Literal["local", "lock", "none"] = "local"
    pool: Literal["fresh", "warm"] = "fresh"

    @model_validator(mode="after")
    def check(self):
        if self.b <= self.a:
            raise ValueError("b must be greater than a")
        if self.method == "simpson" and self.n % 2:
            raise ValueError("simpson needs an even n")
        return self


@app.post("/v1/compute", dependencies=[Depends(auth)])
async def start_compute(body: ComputeIn):
    job_id = str(uuid.uuid4())
    result = await asyncio.to_thread(compute.run, body.a, body.b, body.n, body.workers,
                                     body.method, body.order, body.mode, body.pool)
    job = {"id": job_id, "state": "done", "result": result}
    app.state.jobs[job_id] = job
    return JSONResponse(job, headers={"Location": f"/v1/compute/{job_id}"})


@app.get("/v1/compute/{job_id}", dependencies=[Depends(auth)])
async def get_compute(job_id: str):
    job = app.state.jobs.get(job_id)
    if job is None:
        raise ProblemError(404, "Compute job not found", f"No compute job {job_id}")
    return job


async def set_state(req_id: uuid.UUID, state: str, reason: str | None = None):
    await app.state.pool.execute(
        "UPDATE match_result_request SET state = $2, reason = $3, updated_at = now() WHERE id = $1",
        req_id, state, reason)


async def verify_on_game_server(body: MatchResultIn) -> bool:
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return random.random() > 0.05


async def process(req_id: uuid.UUID, body: MatchResultIn):
    await set_state(req_id, "processing")
    if not await verify_on_game_server(body):
        await set_state(req_id, "rejected", "game server did not confirm the result")
    elif await rating.apply_rating(app.state.pool, req_id, body.match_id, body.winner_id, body.loser_id):
        await set_state(req_id, "done")
    else:
        await set_state(req_id, "rejected", "result for this match is already recorded")


async def worker():
    q = app.state.queue
    while True:
        req_id, body = await q.get()
        try:
            await process(req_id, body)
        except Exception as e:
            try:
                await set_state(req_id, "failed", repr(e)[:500])
            except Exception:
                pass
        finally:
            q.task_done()
