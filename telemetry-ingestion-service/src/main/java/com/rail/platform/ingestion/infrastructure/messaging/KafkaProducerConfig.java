package com.rail.platform.ingestion.infrastructure.messaging;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.protobuf.Message;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

/**
 * Reactor-Kafka producer wiring (infrastructure detail of the messaging
 * adapters). Provides two senders: the protobuf telemetry sender bound to the
 * {@link TripIdPartitioner} and the Schema Registry, and the raw-bytes sender
 * used by the dead-letter adapter.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${rail.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${rail.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    private Map<String, Object> commonProducerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        return props;
    }

    @Bean
    public KafkaSender<String, Message> telemetrySender() {
        Map<String, Object> props = commonProducerProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TripIdPartitioner.class);
        // Schema Registry integration (auto-register in dev; pin schemas in prod)
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", true);

        SenderOptions<String, Message> options = SenderOptions.<String, Message>create(props)
                .maxInFlight(2048);
        return KafkaSender.create(options);
    }

    /**
     * Sender for the dead-letter queue. Carries the original raw bytes so a
     * malformed payload is never lost and can be replayed/inspected.
     */
    @Bean
    public KafkaSender<String, byte[]> dlqSender() {
        Map<String, Object> props = commonProducerProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        SenderOptions<String, byte[]> options = SenderOptions.<String, byte[]>create(props)
                .maxInFlight(512);
        return KafkaSender.create(options);
    }
}
