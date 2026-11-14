import argparse
import csv
import statistics
import time
from pathlib import Path

import httpx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8090")
    ap.add_argument("--token", default="dev-token")
    ap.add_argument("--n", type=int, default=12_000_000)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--method", default="midpoint")
    ap.add_argument("--pool", default="fresh", choices=["fresh", "warm"])
    ap.add_argument("--label", default="baseline")
    ap.add_argument("--out", default="results")
    a = ap.parse_args()

    rows = []
    with httpx.Client(base_url=a.base, headers={"Authorization": f"Bearer {a.token}"}, timeout=900) as c:
        for workers in (1, 2, 4):
            body = {"n": a.n, "workers": workers, "method": a.method, "pool": a.pool}
            c.post("/v1/compute", json={**body, "n": 10_000}).raise_for_status()
            for run in range(1, a.runs + 1):
                t0 = time.perf_counter()
                r = c.post("/v1/compute", json=body)
                http_s = time.perf_counter() - t0
                r.raise_for_status()
                res = r.json()["result"]
                rows.append({"label": a.label, "workers": workers, "run": run, "seconds": res["wall_s"],
                             "http_s": round(http_s, 4), "compute_s": res["timing"]["compute_s"],
                             "overhead_s": res["timing"]["overhead_s"], "error": res["error"]})
                print(f"workers={workers} run={run} wall={res['wall_s']:.3f}s compute={res['timing']['compute_s']:.3f}s "
                      f"overhead={res['timing']['overhead_s']:.3f}s http={http_s:.3f}s error={res['error']:.2e}")

    out = Path(a.out)
    out.mkdir(exist_ok=True)
    path = out / f"speedup-{a.label}.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    by = {p: [r for r in rows if r["workers"] == p] for p in (1, 2, 4)}
    t1 = statistics.median(r["seconds"] for r in by[1])
    print()
    print(f"{a.label}: method={a.method}, n={a.n}, pool={a.pool}, runs={a.runs}; times are wall_s inside the service")
    print("| p | T_p median, s | spread, s | compute median, s | overhead median, s | S_p | E_p |")
    print("|---|---|---|---|---|---|---|")
    for p, rs in by.items():
        secs = [r["seconds"] for r in rs]
        tp = statistics.median(secs)
        sp = t1 / tp
        print(f"| {p} | {tp:.3f} | {max(secs) - min(secs):.3f} | "
              f"{statistics.median(r['compute_s'] for r in rs):.3f} | "
              f"{statistics.median(r['overhead_s'] for r in rs):.3f} | {sp:.2f} | {sp / p:.2f} |")
    print(f"saved to {path}")


if __name__ == "__main__":
    main()
