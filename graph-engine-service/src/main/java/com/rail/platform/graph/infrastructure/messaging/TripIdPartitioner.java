package com.rail.platform.graph.infrastructure.messaging;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

/**
 * Deterministic {@code trip_id}-based partitioner for the cascade producer.
 *
 * <p>Identical to the ingestion gateway's partitioner so cascade events land on
 * the same partition as the journey they describe: a stable {@link Utils#murmur2}
 * hash of the key folded into {@code [0, numPartitions)}. All events for one trip
 * therefore stay in causal order across {@code rail.graph.cascade}.
 */
public class TripIdPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);
        if (keyBytes == null) {
            return Utils.toPositive(topic.hashCode()) % numPartitions;
        }
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration required.
    }

    @Override
    public void close() {
        // No resources held.
    }
}
