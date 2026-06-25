"""Spatio-Temporal Graph Convolutional Network (STGCN).

A compact, faithful implementation of the STGCN architecture (Yu, Yin & Zhu,
IJCAI 2018): stacked *ST-Conv blocks*, each a sandwich of

    [ temporal gated conv ] -> [ spatial graph conv ] -> [ temporal gated conv ]

followed by an output head that collapses the remaining temporal dimension and
maps to a per-node scalar forecast.

The spatial layer is a Chebyshev graph convolution that operates directly on the
**Chebyshev-rescaled Laplacian** L̃ = (2/λmax)·L − I exported by the graph engine
(`GET /graph/spectral?form=scaled`). Because spec(L̃) ⊂ [−1, 1], the Chebyshev
recurrence T_k(L̃) = 2·L̃·T_{k-1} − T_{k-2} is numerically stable, so no
torch_geometric dependency is needed — L̃ is just a sparse tensor we multiply.

Tensor layout throughout: ``x`` has shape ``[B, C, T, N]``
(batch, channels, time, nodes).
"""
from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F


class TemporalGatedConv(nn.Module):
    """Gated 1-D causal-style conv along the time axis with a GLU nonlinearity.

    Maps ``[B, c_in, T, N] -> [B, c_out, T - kt + 1, N]``. The conv produces
    ``2*c_out`` channels which are split into a value branch ``P`` and a gate
    branch ``Q``; the output is ``P ⊙ sigmoid(Q)`` (gated linear unit). A 1×1
    residual aligns the input channels so gradients flow even early in training.
    """

    def __init__(self, c_in: int, c_out: int, kt: int = 3):
        super().__init__()
        self.c_out = c_out
        self.kt = kt
        self.conv = nn.Conv2d(c_in, 2 * c_out, kernel_size=(kt, 1))
        self.residual = nn.Conv2d(c_in, c_out, kernel_size=(1, 1)) if c_in != c_out else None

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        res = x if self.residual is None else self.residual(x)
        res = res[:, :, self.kt - 1:, :]          # align to the conv's valid window
        pq = self.conv(x)
        p, q = torch.split(pq, self.c_out, dim=1)
        return (p + res) * torch.sigmoid(q)


class ChebGraphConv(nn.Module):
    """Chebyshev spectral graph convolution on the rescaled Laplacian L̃.

    For each (batch, time) frame, computes
        sum_{k=0..K-1} Θ_k · (T_k(L̃) x)
    where T_0 = x, T_1 = L̃ x, T_k = 2 L̃ T_{k-1} − T_{k-2}. ``L_tilde`` is a
    sparse ``[N, N]`` tensor; the multiply stays sparse so cost is O(K·nnz·C).
    """

    def __init__(self, c_in: int, c_out: int, order: int):
        super().__init__()
        self.order = order
        self.theta = nn.Parameter(torch.empty(order, c_in, c_out))
        nn.init.xavier_uniform_(self.theta)
        self.bias = nn.Parameter(torch.zeros(c_out))

    def forward(self, x: torch.Tensor, l_tilde: torch.Tensor) -> torch.Tensor:
        b, c, t, n = x.shape
        # Fold (B,C,T) into the "feature rows" so we can do one sparse mm over N.
        # x_nodes: [N, B*T*C]
        x_nodes = x.permute(3, 0, 2, 1).reshape(n, b * t * c)
        tk_prev = x_nodes                       # T_0
        out = self._mix(tk_prev, 0, b, t, c)
        if self.order > 1:
            tk = torch.sparse.mm(l_tilde, x_nodes)  # T_1
            out = out + self._mix(tk, 1, b, t, c)
            for k in range(2, self.order):
                tk_next = 2 * torch.sparse.mm(l_tilde, tk) - tk_prev
                out = out + self._mix(tk_next, k, b, t, c)
                tk_prev, tk = tk, tk_next
        # out currently [N, B*T, c_out] -> back to [B, c_out, T, N]
        out = out.reshape(n, b, t, -1).permute(1, 3, 2, 0)
        return out + self.bias.view(1, -1, 1, 1)

    def _mix(self, tk_nodes: torch.Tensor, k: int, b: int, t: int, c: int) -> torch.Tensor:
        n = tk_nodes.shape[0]
        feats = tk_nodes.reshape(n, b * t, c)           # [N, B*T, c_in]
        return torch.einsum("nbc,co->nbo", feats, self.theta[k])  # [N, B*T, c_out]


class STConvBlock(nn.Module):
    """A temporal-spatial-temporal sandwich with norm + dropout."""

    def __init__(self, c_in: int, c_spatial: int, c_out: int, order: int, kt: int, dropout: float):
        super().__init__()
        self.t1 = TemporalGatedConv(c_in, c_spatial, kt)
        self.gconv = ChebGraphConv(c_spatial, c_spatial, order)
        self.t2 = TemporalGatedConv(c_spatial, c_out, kt)
        self.norm = nn.LayerNorm(c_out)
        self.drop = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor, l_tilde: torch.Tensor) -> torch.Tensor:
        x = self.t1(x)
        x = F.relu(self.gconv(x, l_tilde))
        x = self.t2(x)
        # LayerNorm over the channel axis: move C last, normalize, move back.
        x = self.norm(x.permute(0, 2, 3, 1)).permute(0, 3, 1, 2)
        return self.drop(x)


class STGCN(nn.Module):
    """Two ST-Conv blocks + an output head producing one scalar per node.

    Input  ``[B, c_in, T, N]`` (c_in=1: the delay signal).
    Output ``[B, N]`` — the predicted delay (seconds) at the forecast horizon.
    """

    def __init__(self, n_nodes: int, c_in: int = 1, hidden: int = 32,
                 order: int = 3, kt: int = 3, history: int = 12, dropout: float = 0.1):
        super().__init__()
        self.n_nodes = n_nodes
        self.history = history
        self.block1 = STConvBlock(c_in, hidden, hidden, order, kt, dropout)
        self.block2 = STConvBlock(hidden, hidden, hidden, order, kt, dropout)
        # After two blocks each consuming (kt-1)*2 time steps, collapse what's left.
        t_remaining = history - 4 * (kt - 1)
        if t_remaining < 1:
            raise ValueError(f"history={history} too short for kt={kt} (needs >= {4 * (kt - 1) + 1})")
        self.final_temporal = nn.Conv2d(hidden, hidden, kernel_size=(t_remaining, 1))
        self.head = nn.Sequential(nn.Linear(hidden, hidden), nn.ReLU(), nn.Linear(hidden, 1))

    def forward(self, x: torch.Tensor, l_tilde: torch.Tensor) -> torch.Tensor:
        x = self.block1(x, l_tilde)
        x = self.block2(x, l_tilde)
        x = self.final_temporal(x)          # [B, hidden, 1, N]
        x = x.squeeze(2).permute(0, 2, 1)   # [B, N, hidden]
        return self.head(x).squeeze(-1)     # [B, N]
