package com.rail.platform.graph.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.infrastructure.config.HazelcastConfig;
import com.rail.platform.graph.support.MockNetworks;

/**
 * DoD fault-injection: the topology snapshot must survive a member crash and be
 * available to a freshly (re)started member. Exercises the backed-up IMap that
 * backs {@code HazelcastTopologyRepository}.
 */
class HazelcastFaultInjectionTest {

    private final String clusterName = "rail-graph-test-" + UUID.randomUUID();

    private Config config() {
        Config c = new Config();
        c.setClusterName(clusterName);
        c.setProperty("hazelcast.phone.home.enabled", "false");
        c.setProperty("hazelcast.logging.type", "slf4j");
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        c.addMapConfig(new MapConfig(HazelcastConfig.TOPOLOGY_MAP)
                .setBackupCount(1)
                .setReadBackupData(true));
        return c;
    }

    @AfterEach
    void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    void snapshotSurvivesMemberCrashAndRestart() throws InterruptedException {
        HazelcastInstance a = Hazelcast.newHazelcastInstance(config());
        HazelcastInstance b = Hazelcast.newHazelcastInstance(config());
        awaitClusterSize(b, 2);

        RailTopology topology = MockNetworks.twoLinePareto();
        a.<String, RailTopology>getMap(HazelcastConfig.TOPOLOGY_MAP)
                .put(HazelcastConfig.TOPOLOGY_KEY, topology);

        // Both members can read the snapshot.
        RailTopology onB = b.<String, RailTopology>getMap(HazelcastConfig.TOPOLOGY_MAP)
                .get(HazelcastConfig.TOPOLOGY_KEY);
        assertThat(onB).isNotNull();
        String version = onB.graphVersion();

        // Crash member a (no graceful migration) — backup on b must be promoted.
        a.getLifecycleService().terminate();
        awaitClusterSize(b, 1);

        RailTopology recovered = b.<String, RailTopology>getMap(HazelcastConfig.TOPOLOGY_MAP)
                .get(HazelcastConfig.TOPOLOGY_KEY);
        assertThat(recovered).isNotNull();
        assertThat(recovered.graphVersion()).isEqualTo(version);
        assertThat(recovered.stationCount()).isEqualTo(topology.stationCount());

        // A restarted/new member re-syncs the state from the cluster.
        HazelcastInstance c = Hazelcast.newHazelcastInstance(config());
        awaitClusterSize(c, 2);
        IMap<String, RailTopology> mapC = c.getMap(HazelcastConfig.TOPOLOGY_MAP);
        assertThat(mapC.get(HazelcastConfig.TOPOLOGY_KEY)).isNotNull();
        assertThat(mapC.get(HazelcastConfig.TOPOLOGY_KEY).stationCount())
                .isEqualTo(topology.stationCount());
    }

    private static void awaitClusterSize(HazelcastInstance instance, int size) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (instance.getCluster().getMembers().size() != size) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("cluster did not reach size " + size
                        + " (was " + instance.getCluster().getMembers().size() + ")");
            }
            Thread.sleep(100);
        }
    }
}
