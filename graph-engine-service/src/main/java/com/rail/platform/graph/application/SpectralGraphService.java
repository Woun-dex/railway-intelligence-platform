package com.rail.platform.graph.application;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.SpectralMatrix;

/**
 * Builds the spectral representations the STGCN spatial layer consumes from the
 * rail graph.
 *
 * <p>The directed running graph is first symmetrized into a weighted adjacency
 * {@code W} using a Gaussian kernel on travel time (Yu, Yin &amp; Zhu, 2018):
 * {@code w_ij = exp(-t_ij² / σ²)} so geographically closer stations couple more
 * strongly. From {@code W} it derives the symmetric normalized Laplacian and its
 * Chebyshev-rescaled form, which ChebNet uses to approximate the spectral graph
 * convolution without an eigendecomposition.
 */
@Service
public class SpectralGraphService {

    public enum Form { ADJACENCY, LAPLACIAN, SCALED }

    private final double sigmaConfig;
    private final double epsilon;
    private final int lambdaIterations;

    public SpectralGraphService(
            @Value("${rail.graph.spectral.sigma-sec:0}") double sigmaConfig,
            @Value("${rail.graph.spectral.epsilon:0.0}") double epsilon,
            @Value("${rail.graph.spectral.lambda-iterations:100}") int lambdaIterations) {
        this.sigmaConfig = sigmaConfig;
        this.epsilon = epsilon;
        this.lambdaIterations = lambdaIterations;
    }

    public SpectralMatrix compute(RailTopology t, Form form) {
        int n = t.stationCount();

        // 1. Undirected min travel time per station pair.
        Map<Long, Integer> und = new HashMap<>();
        for (int i = 0; i < n; i++) {
            for (int e = t.adjBegin(i); e < t.adjEnd(i); e++) {
                int j = t.adjTarget(e);
                if (i == j) {
                    continue;
                }
                int a = Math.min(i, j);
                int b = Math.max(i, j);
                und.merge((((long) a) << 32) | b, t.adjWeightSec(e), Math::min);
            }
        }

        // 2. Gaussian-kernel weights (sigma derived from the travel-time spread if not set).
        double sigma = sigmaConfig > 0 ? sigmaConfig : deriveSigma(und.values());
        double inv2Sigma2 = 1.0 / (sigma * sigma);
        int m = und.size();
        int[] ea = new int[m];
        int[] eb = new int[m];
        double[] ew = new double[m];
        double[] deg = new double[n];
        int edgeCount = 0;
        for (Map.Entry<Long, Integer> en : und.entrySet()) {
            int a = (int) (en.getKey() >> 32);
            int b = en.getKey().intValue();
            double w = Math.exp(-(double) en.getValue() * en.getValue() * inv2Sigma2);
            if (w < epsilon) {
                continue;
            }
            ea[edgeCount] = a;
            eb[edgeCount] = b;
            ew[edgeCount] = w;
            edgeCount++;
            deg[a] += w;
            deg[b] += w;
        }

        return switch (form) {
            case ADJACENCY -> renormalizedAdjacency(n, ea, eb, ew, edgeCount, deg);
            case LAPLACIAN -> {
                SpectralMatrix l = normalizedLaplacian(n, ea, eb, ew, edgeCount, deg);
                yield new SpectralMatrix(n, "laplacian", lambdaMax(n, l), l.rows(), l.cols(), l.vals());
            }
            case SCALED -> scaledLaplacian(n, ea, eb, ew, edgeCount, deg);
        };
    }

    // Â = D̃^{-1/2} (W + I) D̃^{-1/2},  D̃ = D + I  (Kipf & Welling renormalization)
    private SpectralMatrix renormalizedAdjacency(int n, int[] ea, int[] eb, double[] ew, int m, double[] deg) {
        double[] dt = new double[n];
        for (int i = 0; i < n; i++) {
            dt[i] = Math.sqrt(deg[i] + 1.0);
        }
        int nnz = n + 2 * m;
        int[] rows = new int[nnz];
        int[] cols = new int[nnz];
        double[] vals = new double[nnz];
        int k = 0;
        for (int i = 0; i < n; i++) {
            rows[k] = i; cols[k] = i; vals[k] = 1.0 / (dt[i] * dt[i]); k++; // (W+I)_ii = 1
        }
        for (int e = 0; e < m; e++) {
            double v = ew[e] / (dt[ea[e]] * dt[eb[e]]);
            rows[k] = ea[e]; cols[k] = eb[e]; vals[k] = v; k++;
            rows[k] = eb[e]; cols[k] = ea[e]; vals[k] = v; k++;
        }
        return new SpectralMatrix(n, "adjacency", 0.0, rows, cols, vals);
    }

    // L = I - D^{-1/2} W D^{-1/2}
    private SpectralMatrix normalizedLaplacian(int n, int[] ea, int[] eb, double[] ew, int m, double[] deg) {
        double[] ds = new double[n];
        for (int i = 0; i < n; i++) {
            ds[i] = deg[i] > 0 ? Math.sqrt(deg[i]) : 0.0;
        }
        int nnz = n + 2 * m;
        int[] rows = new int[nnz];
        int[] cols = new int[nnz];
        double[] vals = new double[nnz];
        int k = 0;
        for (int i = 0; i < n; i++) {
            rows[k] = i; cols[k] = i; vals[k] = deg[i] > 0 ? 1.0 : 0.0; k++;
        }
        for (int e = 0; e < m; e++) {
            double v = -ew[e] / (ds[ea[e]] * ds[eb[e]]);
            rows[k] = ea[e]; cols[k] = eb[e]; vals[k] = v; k++;
            rows[k] = eb[e]; cols[k] = ea[e]; vals[k] = v; k++;
        }
        return new SpectralMatrix(n, "laplacian", 0.0, rows, cols, vals);
    }

    // L̃ = (2/λmax) L - I
    private SpectralMatrix scaledLaplacian(int n, int[] ea, int[] eb, double[] ew, int m, double[] deg) {
        SpectralMatrix l = normalizedLaplacian(n, ea, eb, ew, m, deg);
        double lambda = lambdaMax(n, l);
        double scale = 2.0 / lambda;
        int[] rows = l.rows();
        int[] cols = l.cols();
        double[] src = l.vals();
        double[] vals = new double[src.length];
        for (int k = 0; k < src.length; k++) {
            vals[k] = scale * src[k] - (rows[k] == cols[k] ? 1.0 : 0.0);
        }
        return new SpectralMatrix(n, "scaled", lambda, rows, cols, vals);
    }

    /** Largest eigenvalue of the (PSD, symmetric) normalized Laplacian via power iteration. */
    private double lambdaMax(int n, SpectralMatrix l) {
        if (n == 0) {
            return 2.0;
        }
        double[] x = new double[n];
        java.util.Arrays.fill(x, 1.0 / Math.sqrt(n));
        double lambda = 2.0;
        for (int it = 0; it < lambdaIterations; it++) {
            double[] y = new double[n];
            for (int k = 0; k < l.vals().length; k++) {
                y[l.rows()[k]] += l.vals()[k] * x[l.cols()[k]];
            }
            double rayleigh = 0.0;
            double norm = 0.0;
            for (int i = 0; i < n; i++) {
                rayleigh += x[i] * y[i];
                norm += y[i] * y[i];
            }
            norm = Math.sqrt(norm);
            if (norm == 0.0) {
                return 2.0;
            }
            lambda = rayleigh;
            for (int i = 0; i < n; i++) {
                x[i] = y[i] / norm;
            }
        }
        // Normalized-Laplacian eigenvalues live in [0, 2]; guard against drift.
        return Math.min(2.0, Math.max(1e-6, lambda));
    }

    private static double deriveSigma(Iterable<Integer> times) {
        double sum = 0;
        double sumSq = 0;
        int count = 0;
        for (int t : times) {
            sum += t;
            sumSq += (double) t * t;
            count++;
        }
        if (count == 0) {
            return 1.0;
        }
        double mean = sum / count;
        double var = sumSq / count - mean * mean;
        double std = var > 0 ? Math.sqrt(var) : mean;
        return std > 0 ? std : 1.0;
    }
}
