package com.rail.platform.ingestion.infrastructure.messaging;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * Deterministic {@code trip_id}-based partitioner.
 *
 * <p>Every telemetry event is produced with its {@code trip_id} as the record
 * key. This partitioner binds a record to a partition using a stable hash of
 * that key over the {@code N_p &ge; 22} partitions of the topic. Because the
 * mapping is a pure function of the key, all events for a given journey land on
 * the same partition and are therefore processed in causal order by a single
 * consumer worker — regardless of how many worker threads run in parallel.
 *
 * <p>We use Kafka's {@link Utils#murmur2(byte[])} (the same hash the default
 * partitioner uses) so the mapping is portable and JVM-independent, then fold
 * it into the partition count with {@link Utils#toPositive(int)} to avoid the
 * sign bias of a raw {@code %}. Records with no key are spread by topic, but in
 * normal operation the gateway always supplies a key.
 */
public class TripIdPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);

        if (keyBytes == null) {
            // Defensive fallback: a keyless record cannot preserve per-trip
            // ordering, so spread it deterministically by topic to avoid hot
            // partitions. The gateway guarantees a key in practice.
            return Utils.toPositive(topic.hashCode()) % numPartitions;
        }

        // Stable hash of the trip_id key, folded into [0, numPartitions).
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration required; the partition count is read per-call from
        // the cluster metadata so topic re-partitioning is picked up live.
    }

    @Override
    public void close() {
        // No resources held.
    }
}
