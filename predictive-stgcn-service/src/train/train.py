"""Offline training entry point for the STGCN forecaster.

Fetches the REAL topology from the graph engine (same Chebyshev-scaled Laplacian
the live model uses), bootstraps a digital-twin dataset with
:class:`DelaySimulator`, trains the network to predict the horizon delay
*residual*, and writes a checkpoint + sidecar metadata that the inference engine
auto-loads.

Run inside the image (it has torch):

    docker compose --profile ml run --rm \
        -v "$PWD/predictive-stgcn-service/artifacts:/app/artifacts" \
        stgcn-predictor python -m src.train.train

The target is δ(t+h) − δ(t), divided by ``delay_scale`` for numerical stability;
inference applies the same scale and adds the prediction back onto the current
delay, so a trained checkpoint makes the escalation metric I_P meaningful.
"""
from __future__ import annotations

import json
import logging
import os
import time

import numpy as np
import torch
from torch.utils.data import DataLoader, TensorDataset

from src.config import settings
from src.graph.topology_client import fetch_topology
from src.model.stgcn_network import STGCN
from src.train.simulate import DelaySimulator

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("stgcn.train")


def _pinball(pred: torch.Tensor, target: torch.Tensor, tau: float) -> torch.Tensor:
    """Quantile (pinball) loss — biases the point head toward upside risk."""
    err = target - pred
    return torch.mean(torch.maximum(tau * err, (tau - 1) * err))


def main() -> None:
    cfg = settings
    torch.manual_seed(cfg.train_seed)
    np.random.seed(cfg.train_seed)

    log.info("fetching real topology from %s …", cfg.graph_engine_url)
    topo = fetch_topology(cfg.graph_engine_url)
    horizon_steps = max(1, round(cfg.horizon_seconds / cfg.frame_seconds))
    log.info("topology n=%d nnz=%d version=%s; history=%d horizon_steps=%d",
             topo.n, topo.nnz, topo.graph_version, cfg.history_steps, horizon_steps)

    sim = DelaySimulator(topo.l_tilde_csr.astype(bool).astype(float).tocsr(), seed=cfg.train_seed)
    # NB: we want the *connectivity*, not L̃'s signed values, for the diffusion op.
    x, y = sim.dataset(cfg.train_episodes, cfg.history_steps, horizon_steps, cfg.train_max_sources)
    log.info("dataset X=%s Y=%s (residual seconds: mean=%.1f max=%.1f)",
             x.shape, y.shape, float(np.mean(np.abs(y))), float(np.max(np.abs(y))))

    scale = cfg.delay_scale
    xt = torch.from_numpy(x / scale)
    yt = torch.from_numpy(y / scale)
    n_val = int(cfg.train_val_frac * len(xt))
    perm = torch.randperm(len(xt))
    val_idx, tr_idx = perm[:n_val], perm[n_val:]
    tr = DataLoader(TensorDataset(xt[tr_idx], yt[tr_idx]), batch_size=cfg.train_batch_size, shuffle=True)
    va = DataLoader(TensorDataset(xt[val_idx], yt[val_idx]), batch_size=cfg.train_batch_size)

    model = STGCN(n_nodes=topo.n, c_in=1, hidden=cfg.hidden_channels,
                  order=cfg.cheb_order, kt=3, history=cfg.history_steps)
    l_tilde = topo.l_tilde_torch
    opt = torch.optim.Adam(model.parameters(), lr=cfg.train_lr)
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, factor=0.5, patience=3)

    best_val = float("inf")
    out_path = cfg.checkpoint_path
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    t_start = time.time()

    for epoch in range(1, cfg.train_epochs + 1):
        model.train()
        tr_loss = 0.0
        for xb, yb in tr:
            opt.zero_grad()
            pred = model(xb, l_tilde)
            # 0.7 MSE for calibration + 0.3 pinball@0.9 for upside-risk sensitivity
            loss = 0.7 * torch.nn.functional.mse_loss(pred, yb) + 0.3 * _pinball(pred, yb, 0.9)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 5.0)
            opt.step()
            tr_loss += loss.item() * len(xb)
        tr_loss /= len(tr_idx)

        model.eval()
        val_loss, mae_sec = 0.0, 0.0
        with torch.no_grad():
            for xb, yb in va:
                pred = model(xb, l_tilde)
                val_loss += torch.nn.functional.mse_loss(pred, yb).item() * len(xb)
                mae_sec += torch.mean(torch.abs(pred - yb)).item() * scale * len(xb)
        val_loss /= max(1, len(val_idx))
        mae_sec /= max(1, len(val_idx))
        sched.step(val_loss)
        log.info("epoch %02d/%d  train=%.4f  val=%.4f  val_MAE=%.1fs",
                 epoch, cfg.train_epochs, tr_loss, val_loss, mae_sec)

        if val_loss < best_val:
            best_val = val_loss
            torch.save(model.state_dict(), out_path)
            meta = {
                "graph_version": topo.graph_version, "n_nodes": topo.n,
                "history_steps": cfg.history_steps, "horizon_seconds": cfg.horizon_seconds,
                "cheb_order": cfg.cheb_order, "hidden_channels": cfg.hidden_channels,
                "delay_scale": scale, "val_mse": best_val, "val_mae_seconds": mae_sec,
                "trained_at": int(time.time() * 1000), "source": "digital-twin-simulator",
            }
            with open(out_path + ".json", "w") as f:
                json.dump(meta, f, indent=2)

    log.info("done in %.1fs — best val MSE=%.4f → %s", time.time() - t_start, best_val, out_path)


if __name__ == "__main__":
    main()
