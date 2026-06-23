package com.rail.platform.graph.infrastructure.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Embedded Hazelcast member, hosting the rail topology snapshot for distribution
 * and recovery.
 *
 * <p>The {@code graph-topology} map is configured with a synchronous backup so a
 * single member loss does not lose the snapshot — the property the DoD's
 * fault-injection test exercises. The hot path never reads from this map; the
 * engines read a local in-heap reference held by {@code HazelcastTopologyRepository}.
 */
@Configuration
public class HazelcastConfig {

    public static final String TOPOLOGY_MAP = "graph-topology";
    public static final String TOPOLOGY_KEY = "current";

    @Bean(destroyMethod = "shutdown")
    public HazelcastInstance hazelcastInstance(
            @Value("${rail.graph.hazelcast.cluster-name:rail-graph}") String clusterName,
            @Value("${rail.graph.hazelcast.backup-count:1}") int backupCount) {

        Config config = new Config();
        config.setClusterName(clusterName);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.logging.type", "slf4j");

        // Standalone embedded member: no multicast probing in dev.
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        MapConfig topology = new MapConfig(TOPOLOGY_MAP)
                .setBackupCount(backupCount)
                .setReadBackupData(true);
        config.addMapConfig(topology);

        return Hazelcast.newHazelcastInstance(config);
    }
}
