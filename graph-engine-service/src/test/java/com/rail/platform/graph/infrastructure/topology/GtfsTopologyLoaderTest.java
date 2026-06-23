package com.rail.platform.graph.infrastructure.topology;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.core.io.DefaultResourceLoader;

import com.rail.platform.graph.domain.model.RailTopology;

class GtfsTopologyLoaderTest {

    private GtfsTopologyLoader loader() {
        return new GtfsTopologyLoader(
                new DefaultResourceLoader(),
                new SlackModel(600, 180, 120),
                "classpath:gtfs/transilien-sample",
                "2",
                120);
    }

    @Test
    void loadsRealTransilienSample() {
        RailTopology t = loader().load();

        assertThat(t.stationCount()).isEqualTo(26);
        assertThat(t.routeCount()).isEqualTo(4); // RER A, RER E, Transilien L, Transilien J
        assertThat(t.edgeCount()).isGreaterThan(0);

        int ladef = t.index("LADEF");
        assertThat(ladef).isGreaterThanOrEqualTo(0);
        assertThat(t.stationName(ladef)).isEqualTo("La Defense");
    }

    @Test
    void buildsDirectedAdjacencyFromConsecutiveStops() {
        RailTopology t = loader().load();
        int ladef = t.index("LADEF");
        int cdge = t.index("CDGE"); // RER A: La Defense -> Charles de Gaulle - Etoile

        boolean edge = false;
        int weight = -1;
        for (int e = t.adjBegin(ladef); e < t.adjEnd(ladef); e++) {
            if (t.adjTarget(e) == cdge) {
                edge = true;
                weight = t.adjWeightSec(e);
            }
        }
        assertThat(edge).isTrue();
        assertThat(weight).isGreaterThan(0);
    }

    @Test
    void loadsFootTransfers() {
        RailTopology t = loader().load();
        int psl = t.index("PSL");  // Paris Saint-Lazare
        int hsl = t.index("HSL");  // Haussmann - Saint-Lazare

        boolean transfer = false;
        for (int x = t.xferBegin(psl); x < t.xferEnd(psl); x++) {
            if (t.xferTarget(x) == hsl) {
                transfer = true;
            }
        }
        assertThat(transfer).isTrue();
    }

    @Test
    void parsesRouteTypeFilter() {
        assertThat(GtfsTopologyLoader.parseRouteTypes("1, 2 ,3"))
                .containsExactlyInAnyOrder(1, 2, 3);
    }

    // ---- Full IDFM feed integration tests (conditional) --------------------

    private static final String IDFM_PATH = "../data/gtfs/IDFM-gtfs.zip";

    static boolean idfmFeedPresent() {
        return new File(IDFM_PATH).exists();
    }

    private GtfsTopologyLoader idfmLoader() {
        return new GtfsTopologyLoader(
                new DefaultResourceLoader(),
                new SlackModel(600, 180, 120),
                "file:" + IDFM_PATH,
                "2",
                120);
    }

    @Test
    @EnabledIf("idfmFeedPresent")
    void loadsFullIdfmFeed() {
        RailTopology t = idfmLoader().load();

        // The real IDFM rail network should have well over 200 stations
        assertThat(t.stationCount()).isGreaterThan(200);
        // At least 20 rail routes (RER A-E, Transilien H/J/K/L/N/P/R/U/V, TER)
        assertThat(t.routeCount()).isGreaterThanOrEqualTo(20);
        assertThat(t.edgeCount()).isGreaterThan(t.stationCount());
    }

    @Test
    @EnabledIf("idfmFeedPresent")
    void idfmContainsKnownStations() {
        RailTopology t = idfmLoader().load();

        // Verify well-known stations are present by checking station names
        boolean foundGareDuNord = false;
        boolean foundLaDefense = false;
        for (int i = 0; i < t.stationCount(); i++) {
            String name = t.stationName(i).toLowerCase();
            if (name.contains("gare du nord")) foundGareDuNord = true;
            if (name.contains("fense")) foundLaDefense = true; // La Défense (accent-safe)
        }
        assertThat(foundGareDuNord).as("Gare du Nord should be in the network").isTrue();
        assertThat(foundLaDefense).as("La Défense should be in the network").isTrue();
    }
}
