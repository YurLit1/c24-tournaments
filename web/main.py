import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

INSTANCE = os.getenv("INSTANCE", "local")
HOLD_MINUTES = 15


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=10)
    yield
    await app.state.pool.close()


app = FastAPI(title="C24 Tournaments — Web", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5500", "http://127.0.0.1:5500"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Location", "X-Request-ID"],
)



@app.middleware("http")
async def access_log(request: Request, call_next):
    rid = request.headers.get("x-request-id") or str(uuid.uuid4())
    start = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    print(json.dumps({
        "time": datetime.now(timezone.utc).isoformat(),
        "instance": INSTANCE,
        "request_id": rid,
        "method": request.method,
        "route": route.path if route else request.url.path,
        "status": response.status_code,
        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
    }), flush=True)
    response.headers["X-Request-ID"] = rid
    response.headers["X-Instance"] = INSTANCE
    return response



class ProblemError(Exception):
    def __init__(self, status: int, title: str, detail: str):
        self.status, self.title, self.detail = status, title, detail


def problem(request: Request, status: int, title: str, detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        media_type="application/problem+json",
        content={"type": "about:blank", "title": title, "status": status,
                 "detail": detail, "instance": request.url.path},
    )


@app.exception_handler(ProblemError)
async def on_problem(request: Request, exc: ProblemError):
    return problem(request, exc.status, exc.title, exc.detail)


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
    return problem(request, 500, "Internal Server Error", "Unexpected error")


def to_json(row: asyncpg.Record) -> dict:
    out = {}
    for k, v in dict(row).items():
        if isinstance(v, uuid.UUID):
            v = str(v)
        elif isinstance(v, datetime):
            v = v.isoformat()
        out[k] = v
    return out



@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE}


@app.get("/v1/tournaments")
async def list_tournaments():
    rows = await app.state.pool.fetch("""
        SELECT id, game, starts_at, time_zone, slots, available, fee_minor,
               currency, registration_open
        FROM tournament ORDER BY starts_at, id""")
    return {"items": [to_json(r) for r in rows]}


class RegistrationIn(BaseModel):
    tournament_id: uuid.UUID
    nickname: str = Field(min_length=3, max_length=32)


REG_COLUMNS = "id, tournament_id, nickname, status, version, created_at, expires_at"


@app.post("/v1/registrations", status_code=201)
async def create_registration(body: RegistrationIn, x_user_id: uuid.UUID = Header()):
    async with app.state.pool.acquire() as conn:
        async with conn.transaction():
            seat = await conn.fetchval("""
                UPDATE tournament SET available = available - 1
                WHERE id = $1 AND registration_open AND available > 0 AND starts_at > now()
                RETURNING id""", body.tournament_id)
            if seat is None:
                if not await conn.fetchval("SELECT 1 FROM tournament WHERE id = $1", body.tournament_id):
                    raise ProblemError(404, "Tournament not found", f"No tournament {body.tournament_id}")
                raise ProblemError(409, "No free slots", "Registration is closed or all slots are taken")
            try:
                reg = await conn.fetchrow(f"""
                    INSERT INTO registration (user_id, tournament_id, nickname, status, expires_at)
                    VALUES ($1, $2, $3, 'held', now() + make_interval(mins => $4))
                    RETURNING {REG_COLUMNS}""",
                    x_user_id, body.tournament_id, body.nickname, HOLD_MINUTES)
            except asyncpg.UniqueViolationError:
                raise ProblemError(409, "Already registered",
                                   "Active registration of this player or this nickname already exists")
            except asyncpg.ForeignKeyViolationError:
                raise ProblemError(422, "Unknown user", "X-User-Id does not match an existing user")
            except asyncpg.CheckViolationError:
                raise ProblemError(422, "Invalid data", "Registration violates a data constraint")
    return JSONResponse(status_code=201, content=to_json(reg),
                        headers={"Location": f"/v1/registrations/{reg['id']}"})


@app.get("/v1/registrations/{registration_id}")
async def get_registration(registration_id: uuid.UUID, x_user_id: uuid.UUID = Header()):
    reg = await app.state.pool.fetchrow(
        f"SELECT {REG_COLUMNS} FROM registration WHERE id = $1 AND user_id = $2",
        registration_id, x_user_id)
    if reg is None:
        raise ProblemError(404, "Registration not found", f"No registration {registration_id}")
    return to_json(reg)
