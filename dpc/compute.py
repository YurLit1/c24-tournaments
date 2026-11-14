import math
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor

N0 = float(os.getenv("N0", "1200"))
N1 = float(os.getenv("N1", "400"))
PERIOD = 84.0
BLOCK = int(os.getenv("SHARED_BLOCK", "200"))
START_METHOD = os.getenv("MP_START", "forkserver")

_shared = None
_warm_pools: dict[int, ProcessPoolExecutor] = {}


def f(t: float) -> float:
    return N0 + N1 * math.cos(math.pi * t / PERIOD)


def antiderivative(t: float) -> float:
    return N0 * t + PERIOD * N1 / math.pi * math.sin(math.pi * t / PERIOD)


def reference(a: float, b: float) -> float:
    return antiderivative(b) - antiderivative(a)


def node_count(method: str, n: int) -> int:
    return n if method == "midpoint" else n + 1


def split(total: int, parts: int) -> list[tuple[int, int]]:
    base, extra = divmod(total, parts)
    bounds, lo = [], 0
    for k in range(parts):
        hi = lo + base + (1 if k < extra else 0)
        bounds.append((lo, hi))
        lo = hi
    return bounds


def _init(shared) -> None:
    global _shared
    _shared = shared


def _weight(method: str, i: int, n: int) -> float:
    if method == "midpoint":
        return 1.0
    if i == 0 or i == n:
        return 0.5 if method == "trapezoid" else 1.0
    if method == "trapezoid":
        return 1.0
    return 4.0 if i % 2 else 2.0


def _range_sum(a: float, h: float, method: str, n: int, lo: int, hi: int, reverse: bool) -> float:
    c = math.pi / PERIOD
    cos = math.cos
    n0, n1 = N0, N1
    shift = 0.5 if method == "midpoint" else 0.0
    idx = range(hi - 1, lo - 1, -1) if reverse else range(lo, hi)
    s = 0.0
    if method == "midpoint":
        for i in idx:
            s += n0 + n1 * cos(c * (a + (i + shift) * h))
    else:
        for i in idx:
            s += _weight(method, i, n) * (n0 + n1 * cos(c * (a + i * h)))
    return s


def part(a: float, h: float, method: str, n: int, lo: int, hi: int, reverse: bool, mode: str) -> dict:
    started = time.perf_counter()
    if mode == "local":
        total = _range_sum(a, h, method, n, lo, hi, reverse)
        updates = 1
    else:
        total, updates = 0.0, 0
        blocks = [(s, min(s + BLOCK, hi)) for s in range(lo, hi, BLOCK)]
        if reverse:
            blocks.reverse()
        for s, e in blocks:
            block_sum = _range_sum(a, h, method, n, s, e, reverse)
            if mode == "lock":
                with _shared.get_lock():
                    _shared.value += block_sum
            else:
                current = _shared.value
                _shared.value = current + block_sum
            total += block_sum
            updates += 1
    return {"pid": os.getpid(), "lo": lo, "hi": hi, "sum": total, "updates": updates,
            "compute_s": time.perf_counter() - started}


def _noop(_: int = 0) -> int:
    return os.getpid()


def warm_pool(workers: int) -> ProcessPoolExecutor:
    pool = _warm_pools.get(workers)
    if pool is None:
        pool = ProcessPoolExecutor(max_workers=workers, mp_context=mp.get_context(START_METHOD))
        list(pool.map(_noop, range(workers * 4)))
        _warm_pools[workers] = pool
    return pool


def run(a: float, b: float, n: int, workers: int, method: str, order: str, mode: str,
        pool_kind: str = "fresh") -> dict:
    if pool_kind == "warm" and mode == "local":
        warm_pool(workers)
    started = time.perf_counter()
    h = (b - a) / n
    reverse = order == "reverse"
    ctx = mp.get_context(START_METHOD)
    shared = ctx.Value("d", 0.0, lock=(mode == "lock")) if mode != "local" else None
    ranges = split(node_count(method, n), workers)

    if pool_kind == "warm" and mode == "local":
        pool = _warm_pools[workers]
        futures = [pool.submit(part, a, h, method, n, lo, hi, reverse, mode) for lo, hi in ranges]
        parts = [fut.result() for fut in futures]
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=ctx, initializer=_init,
                                 initargs=(shared,)) as pool:
            futures = [pool.submit(part, a, h, method, n, lo, hi, reverse, mode) for lo, hi in ranges]
            parts = [fut.result() for fut in futures]

    merge_started = time.perf_counter()
    if mode == "local":
        ordered = list(reversed(parts)) if reverse else parts
        total = 0.0
        for p in ordered:
            total += p["sum"]
    else:
        total = shared.value
    scale = h / 3.0 if method == "simpson" else h
    value = total * scale
    merge_s = time.perf_counter() - merge_started

    ref = reference(a, b)
    wall_s = time.perf_counter() - started
    compute_s = max(p["compute_s"] for p in parts)
    expected = sum(p["sum"] for p in parts) * scale
    return {
        "value": value,
        "reference": ref,
        "error": abs(value - ref),
        "wall_s": round(wall_s, 4),
        "workers": workers,
        "method": method,
        "order": order,
        "mode": mode,
        "pool": pool_kind if mode == "local" else "fresh",
        "n": n,
        "timing": {
            "compute_s": round(compute_s, 4),
            "overhead_s": round(wall_s - compute_s, 4),
            "merge_s": round(merge_s, 6),
        },
        "parts": [{"lo": p["lo"], "hi": p["hi"], "updates": p["updates"],
                   "compute_s": round(p["compute_s"], 4)} for p in parts],
        "lost": abs(value - expected) if mode != "local" else 0.0,
    }
