import argparse

import httpx

TOLERANCE = 1e-9


def post(c: httpx.Client, **body) -> dict:
    r = c.post("/v1/compute", json=body)
    r.raise_for_status()
    return r.json()["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8090")
    ap.add_argument("--token", default="dev-token")
    ap.add_argument("--n", type=int, default=2_000_000)
    ap.add_argument("--runs", type=int, default=3)
    a = ap.parse_args()

    ok = True
    with httpx.Client(base_url=a.base, headers={"Authorization": f"Bearer {a.token}"}, timeout=600) as c:
        print(f"O2.1: a=0, b=168, n={a.n}, relative tolerance {TOLERANCE:g}")
        print("| method | workers | value | reference | error | relative | ok |")
        print("|---|---|---|---|---|---|---|")
        for method in ("midpoint", "trapezoid", "simpson"):
            for workers in (1, 2, 4):
                r = post(c, a=0, b=168, n=a.n, workers=workers, method=method)
                rel = r["error"] / abs(r["reference"])
                good = rel <= TOLERANCE
                ok &= good
                print(f"| {method} | {workers} | {r['value']:.9f} | {r['reference']:.9f} | {r['error']:.2e} "
                      f"| {rel:.2e} | {'yes' if good else 'NO'} |")

        print()
        print(f"O2.4: midpoint, workers=4, n={a.n}, {a.runs} runs per mode")
        print("| mode | run | value | error | relative | within tolerance |")
        print("|---|---|---|---|---|---|")
        for mode in ("none", "lock", "local"):
            for run in range(1, a.runs + 1):
                r = post(c, a=0, b=168, n=a.n, workers=4, method="midpoint", mode=mode)
                rel = r["error"] / abs(r["reference"])
                good = rel <= TOLERANCE
                if mode != "none":
                    ok &= good
                print(f"| {mode} | {run} | {r['value']:.6f} | {r['error']:.3e} | {rel:.2e} | {'yes' if good else 'no'} |")
    print()
    print("ALL PROTECTED RESULTS WITHIN TOLERANCE" if ok else "SOME PROTECTED RESULT IS OUT OF TOLERANCE")


if __name__ == "__main__":
    main()
