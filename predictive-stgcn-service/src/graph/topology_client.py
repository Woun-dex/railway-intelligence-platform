"""Static topology / adjacency loader.

The graph engine owns the canonical rail topology. Rather than re-derive the
Laplacian here, we fetch the **Chebyshev-rescaled Laplacian** L̃ it already
computes (`GET /graph/spectral?form=scaled`) as a sparse COO payload, and
materialize it as a SciPy sparse matrix (the "optimized format" the milestone
asks for) plus a torch sparse tensor for the model.

Keeping a single source of truth for the graph means the predictor's spatial
operator always matches the deterministic engine's view of the network — same
station indexing, same `graph_version`.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx
import numpy as np
import scipy.sparse as sp
import torch

log = logging.getLogger("stgcn.topology")


@dataclass
class Topology:
    n: int
    graph_version: str
    lambda_max: float
    # SciPy sparse forms of L̃ (the optimized adjacency representation).
    l_tilde_coo: sp.coo_matrix
    l_tilde_csr: sp.csr_matrix
    # Torch sparse tensor used by the model's Chebyshev conv.
    l_tilde_torch: torch.sparse.Tensor
    station_names: list[str]
    pagerank: np.ndarray

    @property
    def nnz(self) -> int:
        return int(self.l_tilde_coo.nnz)


def _to_torch_sparse(coo: sp.coo_matrix) -> torch.sparse.Tensor:
    idx = np.vstack((coo.row, coo.col)).astype(np.int64)
    indices = torch.from_numpy(idx)
    values = torch.from_numpy(coo.data.astype(np.float32))
    return torch.sparse_coo_tensor(indices, values, size=coo.shape).coalesce()


def fetch_topology(graph_engine_url: str, timeout: float = 30.0) -> Topology:
    """Pull L̃ (COO) and station metadata from the graph engine and build the
    sparse spatial operator. Raises on a non-200 / empty topology so the caller
    can retry while the engine is still loading GTFS."""
    base = graph_engine_url.rstrip("/")
    with httpx.Client(timeout=timeout) as client:
        spec = client.get(f"{base}/graph/spectral", params={"form": "scaled"})
        spec.raise_for_status()
        s = spec.json()
        meta = client.get(f"{base}/graph/matrix")
        meta.raise_for_status()
        m = meta.json()

    n = int(s["n"])
    rows = np.asarray(s["rows"], dtype=np.int64)
    cols = np.asarray(s["cols"], dtype=np.int64)
    vals = np.asarray(s["vals"], dtype=np.float64)
    if n == 0 or vals.size == 0:
        raise ValueError("graph engine returned an empty spectral matrix")

    coo = sp.coo_matrix((vals, (rows, cols)), shape=(n, n))
    csr = coo.tocsr()

    stations = m.get("stations", [])
    names = [st.get("name", f"#{i}") for i, st in enumerate(stations)]
    pr = np.asarray([float(st.get("pagerank", 0.0)) for st in stations], dtype=np.float64)
    if pr.size != n:  # defensive: pad/truncate to the spectral matrix order
        pr = np.resize(pr, n)

    topo = Topology(
        n=n,
        graph_version=str(m.get("graphVersion", s.get("form", "unknown"))),
        lambda_max=float(s.get("lambdaMax", 2.0)),
        l_tilde_coo=coo,
        l_tilde_csr=csr,
        l_tilde_torch=_to_torch_sparse(coo),
        station_names=names,
        pagerank=pr,
    )
    log.info("topology loaded: n=%d nnz=%d version=%s lambda_max=%.4f",
             topo.n, topo.nnz, topo.graph_version, topo.lambda_max)
    return topo
