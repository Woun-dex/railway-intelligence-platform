package com.rail.platform.graph.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.RailTopologyBuilder;
import com.rail.platform.graph.domain.model.SpectralMatrix;

class SpectralGraphServiceTest {

    /** Undirected path 0-1-2 with equal travel times. */
    private RailTopology path3() {
        RailTopologyBuilder b = new RailTopologyBuilder();
        for (int i = 0; i < 3; i++) {
            b.station("S" + i, "S" + i, i, 0);
        }
        b.segment(0, 1, 120, 0);
        b.segment(1, 2, 120, 0);
        return b.build();
    }

    private SpectralGraphService service() {
        return new SpectralGraphService(0, 0.0, 200);
    }

    private static double at(SpectralMatrix m, int r, int c) {
        double sum = 0;
        for (int k = 0; k < m.vals().length; k++) {
            if (m.rows()[k] == r && m.cols()[k] == c) {
                sum += m.vals()[k];
            }
        }
        return sum;
    }

    @Test
    void normalizedLaplacianHasExpectedStructure() {
        SpectralMatrix l = service().compute(path3(), SpectralGraphService.Form.LAPLACIAN);

        // L = I - D^{-1/2} W D^{-1/2}; degrees are 1,2,1 so L_01 = -1/sqrt(1*2).
        assertThat(at(l, 0, 0)).isCloseTo(1.0, within(1e-9));
        assertThat(at(l, 1, 1)).isCloseTo(1.0, within(1e-9));
        assertThat(at(l, 0, 1)).isCloseTo(-1.0 / Math.sqrt(2), within(1e-9));
        assertThat(at(l, 1, 0)).isCloseTo(-1.0 / Math.sqrt(2), within(1e-9)); // symmetric
        assertThat(at(l, 0, 2)).isEqualTo(0.0);                                // not adjacent
        assertThat(l.nnz()).isEqualTo(3 + 2 * 2);
    }

    @Test
    void lambdaMaxIsTwoForBipartitePath() {
        SpectralMatrix l = service().compute(path3(), SpectralGraphService.Form.LAPLACIAN);
        // A path is bipartite => largest normalized-Laplacian eigenvalue is exactly 2.
        assertThat(l.lambdaMax()).isCloseTo(2.0, within(1e-3));
    }

    @Test
    void scaledLaplacianSpectrumIsChebyshevReady() {
        SpectralMatrix s = service().compute(path3(), SpectralGraphService.Form.SCALED);

        // L~ = (2/lambdaMax) L - I; with lambdaMax=2 the diagonal collapses to 0.
        assertThat(s.form()).isEqualTo("scaled");
        assertThat(at(s, 0, 0)).isCloseTo(0.0, within(1e-3));
        assertThat(at(s, 0, 1)).isCloseTo(-1.0 / Math.sqrt(2), within(1e-3));
        // every value must lie in [-1, 1] for the Chebyshev recurrence to be stable
        for (double v : s.vals()) {
            assertThat(v).isBetween(-1.0001, 1.0001);
        }
    }

    @Test
    void renormalizedAdjacencyIsSymmetricAndPositive() {
        SpectralMatrix a = service().compute(path3(), SpectralGraphService.Form.ADJACENCY);

        assertThat(a.form()).isEqualTo("adjacency");
        assertThat(at(a, 0, 1)).isGreaterThan(0.0);
        assertThat(at(a, 0, 1)).isCloseTo(at(a, 1, 0), within(1e-12)); // symmetric
        assertThat(at(a, 0, 0)).isGreaterThan(0.0);                     // self-loop from renormalization
        assertThat(at(a, 0, 2)).isEqualTo(0.0);
    }
}
