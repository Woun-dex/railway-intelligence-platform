"""Simulated-load latency probe for the DoD check.

Fires concurrent /predict requests at the running service and reports the
end-to-end inference latency distribution (the service computes per-call P50/95/99
over `runs` forwards; this aggregates across concurrent clients).

    python scripts/loadgen.py --url http://localhost:8092 --clients 16 --rounds 50
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import time

import httpx


async def _worker(client: httpx.AsyncClient, url: str, rounds: int, runs: int, out: list[float]):
    for _ in range(rounds):
        t0 = time.perf_counter()
        r = await client.post(f"{url}/predict", params={"runs": runs})
        r.raise_for_status()
        out.append(r.json()["p99_ms"])
        # also record wall-clock round-trip for an end-to-end view
        out.append((time.perf_counter() - t0) * 1000.0 / runs)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8092")
    ap.add_argument("--clients", type=int, default=16)
    ap.add_argument("--rounds", type=int, default=50)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--budget-ms", type=float, default=35.0)
    args = ap.parse_args()

    server_p99: list[float] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        # confirm readiness first
        h = (await client.get(f"{args.url}/health")).json()
        print(f"service: status={h['status']} nodes={h.get('nodes')} "
              f"graph={h.get('graph_version')}")
        t0 = time.perf_counter()
        await asyncio.gather(*[
            _worker(client, args.url, args.rounds, args.runs, server_p99)
            for _ in range(args.clients)
        ])
        wall = time.perf_counter() - t0

    server_p99.sort()
    p99 = server_p99[int(0.99 * (len(server_p99) - 1))]
    calls = args.clients * args.rounds * args.runs
    print(f"\nclients={args.clients} rounds={args.rounds} runs/req={args.runs} "
          f"-> {calls} forwards in {wall:.1f}s ({calls/wall:.0f}/s)")
    print(f"latency  p50={statistics.median(server_p99):.2f}ms  "
          f"p99={p99:.2f}ms  max={server_p99[-1]:.2f}ms")
    verdict = "PASS" if p99 <= args.budget_ms else "FAIL"
    print(f"DoD P99 <= {args.budget_ms}ms : {verdict}")
    raise SystemExit(0 if verdict == "PASS" else 1)


if __name__ == "__main__":
    asyncio.run(main())
