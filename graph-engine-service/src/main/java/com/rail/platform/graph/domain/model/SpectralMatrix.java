package com.rail.platform.graph.domain.model;

/**
 * A sparse symmetric matrix derived from the rail graph for the STGCN spatial
 * layer, in COO form ({@code vals[k]} sits at row {@code rows[k]}, col
 * {@code cols[k]}).
 *
 * <p>{@code form} is one of:
 * <ul>
 *   <li>{@code adjacency} — renormalized GCN propagation matrix
 *       {@code Â = D̃^{-1/2}(W+I)D̃^{-1/2}};</li>
 *   <li>{@code laplacian} — symmetric normalized Laplacian
 *       {@code L = I − D^{-1/2} W D^{-1/2}};</li>
 *   <li>{@code scaled} — Chebyshev-ready rescaled Laplacian
 *       {@code L̃ = (2/λmax) L − I}, whose spectrum lies in {@code [-1, 1]} so the
 *       Chebyshev recurrence {@code T_k(L̃) = 2 L̃ T_{k-1} − T_{k-2}} is stable.</li>
 * </ul>
 *
 * @param lambdaMax largest eigenvalue of the normalized Laplacian (≈2 for a
 *                  bipartite graph) used for the Chebyshev rescaling
 */
public record SpectralMatrix(int n, String form, double lambdaMax,
                             int[] rows, int[] cols, double[] vals) {

    public int nnz() {
        return vals.length;
    }
}
