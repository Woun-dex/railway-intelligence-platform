"""Shared fixtures: a small synthetic ring-network topology so the model tests
run without a live graph engine."""
from __future__ import annotations

import numpy as np
import pytest
import scipy.sparse as sp
import torch

from src.graph.topology_client import Topology, _to_torch_sparse


def _scaled_laplacian(adj: sp.csr_matrix) -> tuple[sp.coo_matrix, float]:
    """L̃ = (2/λmax)·L − I from a symmetric adjacency (matches the graph engine)."""
    n = adj.shape[0]
    deg = np.asarray(adj.sum(axis=1)).ravel()
    d_inv_sqrt = np.divide(1.0, np.sqrt(deg), out=np.zeros_like(deg), where=deg > 0)
    d_mat = sp.diags(d_inv_sqrt)
    lap = sp.eye(n) - d_mat @ adj @ d_mat            # normalized Laplacian
    lam_max = float(np.real(sp.linalg.eigsh(lap, k=1, which="LM", return_eigenvectors=False)[0]))
    lam_max = max(lam_max, 1e-6)
    l_tilde = (2.0 / lam_max) * lap - sp.eye(n)
    return l_tilde.tocoo(), lam_max


@pytest.fixture
def ring_topology() -> Topology:
    n = 24
    rng = np.random.default_rng(7)
    rows, cols = [], []
    for i in range(n):                                # ring + a few chords
        j = (i + 1) % n
        rows += [i, j]
        cols += [j, i]
    for _ in range(6):
        a, b = rng.integers(0, n, size=2)
        if a != b:
            rows += [a, b]
            cols += [b, a]
    data = np.ones(len(rows), dtype=np.float64)
    adj = sp.coo_matrix((data, (rows, cols)), shape=(n, n)).tocsr()
    adj = (adj + adj.T) / 2
    coo, lam = _scaled_laplacian(adj)
    return Topology(
        n=n, graph_version="test-1", lambda_max=lam,
        l_tilde_coo=coo, l_tilde_csr=coo.tocsr(),
        l_tilde_torch=_to_torch_sparse(coo),
        station_names=[f"S{i}" for i in range(n)],
        pagerank=np.full(n, 1.0 / n),
    )
