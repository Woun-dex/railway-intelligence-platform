package com.rail.platform.ingestion.infrastructure.messaging;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class TripIdPartitionerTest {

    private static final String TOPIC = "rail.raw.position";
    private static final int PARTITIONS = 22;

    private TripIdPartitioner partitioner;
    private Cluster cluster;
    private final StringSerializer ser = new StringSerializer();

    @BeforeEach
    void setUp() {
        partitioner = new TripIdPartitioner();
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            partitions.add(new PartitionInfo(TOPIC, i, node, new Node[]{node}, new Node[]{node}));
        }
        cluster = new Cluster("rail", List.of(node), partitions, Set.of(), Set.of());
    }

    @AfterEach
    void tearDown() {
        partitioner.close();
        ser.close();
    }

    private int partitionFor(String tripId) {
        byte[] keyBytes = ser.serialize(TOPIC, tripId);
        return partitioner.partition(TOPIC, tripId, keyBytes, null, null, cluster);
    }

    @Test
    void mapsSameTripToSamePartitionEveryTime() {
        String tripId = "RER-E:2026-06-22:T4471";
        int first = partitionFor(tripId);
        for (int i = 0; i < 1000; i++) {
            assertThat(partitionFor(tripId)).isEqualTo(first);
        }
        assertThat(first).isBetween(0, PARTITIONS - 1);
    }

    @Test
    void spreadsDistinctTripsAcrossAllPartitions() {
        Set<Integer> hit = new HashSet<>();
        for (int i = 0; i < 5000; i++) {
            hit.add(partitionFor("TRIP-" + i));
        }
        // With 5000 keys over 22 partitions, every partition should be reached.
        assertThat(hit).hasSize(PARTITIONS);
    }

    @Test
    void keylessRecordsStillYieldValidPartition() {
        int p = partitioner.partition(TOPIC, null, null, null, null, cluster);
        assertThat(p).isBetween(0, PARTITIONS - 1);
    }
}
