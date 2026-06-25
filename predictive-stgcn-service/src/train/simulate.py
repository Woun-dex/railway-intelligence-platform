"""Digital-twin delay simulator — bootstraps training data from the real network.

There is no historical real-time archive yet, so we synthesize physically-plausible
spatio-temporal delay sequences by rolling out a disruption process *on the real
topology*: delays are injected at random stations, diffuse to neighbours along the
graph (weighted by inverse travel time), grow then recover with inertia, and pick
up observation noise. This is the standard cold-start technique — train on a
digital twin of the propagation dynamics, then fine-tune / retrain on real history
once it accrues. The output matches exactly what the live service sees: a
``[T, N]`` matrix of per-station delays in seconds.

The simulator consumes the same adjacency the model uses (so what it learns is the
real network's spatial structure), but operates on a row-normalized neighbour
diffusion matrix rather than the Chebyshev Laplacian.
"""
from __future__ import annotations

import numpy as np
import scipy.sparse as sp


class DelaySimulator:
    def __init__(self, adj_csr: sp.csr_matrix, seed: int = 0):
        self.n = adj_csr.shape[0]
        self.rng = np.random.default_rng(seed)
        # Row-normalized diffusion operator P (rows sum to 1 where degree > 0).
        adj = adj_csr.copy().astype(np.float64)
        adj.setdiag(0)
        adj.eliminate_zeros()
        deg = np.asarray(adj.sum(axis=1)).ravel()
        inv = np.divide(1.0, deg, out=np.zeros_like(deg), where=deg > 0)
        self.P = sp.diags(inv) @ adj          # [N,N] right-stochastic

    def episode(self, steps: int, max_sources: int = 4) -> np.ndarray:
        """Roll out one disruption episode → ``[steps, N]`` delays (seconds)."""
        n = self.n
        delay = np.zeros(n, dtype=np.float64)
        # Persistent "pressure" per source that injects delay for a while.
        n_src = int(self.rng.integers(1, max_sources + 1))
        sources = self.rng.choice(n, size=n_src, replace=False)
        peak = self.rng.uniform(180, 900, size=n_src)         # 3–15 min disruptions
        onset = self.rng.integers(0, max(1, steps // 2), size=n_src)
        duration = self.rng.integers(steps // 4, steps, size=n_src)

        alpha = self.rng.uniform(0.25, 0.55)   # diffusion strength to neighbours
        recover = self.rng.uniform(0.80, 0.94) # per-step retention (inertia/recovery)

        out = np.zeros((steps, n), dtype=np.float32)
        for t in range(steps):
            # spatial diffusion: a fraction of each node's delay spreads to neighbours
            spread = alpha * (self.P.T @ delay)
            delay = recover * delay + spread
            # active injections at the source stations
            for k, s in enumerate(sources):
                if onset[k] <= t < onset[k] + duration[k]:
                    ramp = min(1.0, (t - onset[k] + 1) / 5.0)
                    delay[s] += 0.15 * peak[k] * ramp
            delay = np.clip(delay, 0, 3600)
            # observation noise
            noisy = delay + self.rng.normal(0, 8.0, size=n)
            out[t] = np.clip(noisy, 0, 3600)
        return out

    def dataset(self, episodes: int, history: int, horizon_steps: int,
                max_sources: int = 4) -> tuple[np.ndarray, np.ndarray]:
        """Build supervised windows.

        Returns ``X [S, 1, history, N]`` (absolute delay history) and
        ``Y [S, N]`` (the *residual* delay at the horizon: δ(t+h) − δ(t)), which is
        exactly the quantity the inference engine adds onto the current delay.
        """
        steps = history + horizon_steps + 2
        xs, ys = [], []
        for _ in range(episodes):
            seq = self.episode(steps, max_sources)            # [steps, N]
            t0 = history - 1
            hist = seq[t0 - history + 1: t0 + 1]               # [history, N]
            cur = seq[t0]                                      # [N]
            fut = seq[t0 + horizon_steps]                      # [N]
            xs.append(hist[None, :, :])                        # [1, history, N]
            ys.append((fut - cur))                             # residual [N]
        x = np.stack(xs).astype(np.float32)                   # [S, 1, history, N]
        y = np.stack(ys).astype(np.float32)                   # [S, N]
        return x, y
