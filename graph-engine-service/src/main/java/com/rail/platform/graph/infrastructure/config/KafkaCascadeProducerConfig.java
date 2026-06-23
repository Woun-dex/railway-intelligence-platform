package com.rail.platform.graph.infrastructure.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.protobuf.Message;
import com.rail.platform.graph.infrastructure.messaging.TripIdPartitioner;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

/**
 * Reactor-Kafka producer wiring for cascade events. Mirrors the ingestion
 * gateway's protobuf sender: Schema-Registry-backed {@link KafkaProtobufSerializer},
 * {@code trip_id} partitioning, idempotent {@code acks=all} delivery.
 */
@Configuration
public class KafkaCascadeProducerConfig {

    @Value("${rail.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${rail.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Bean
    public KafkaSender<String, Message> cascadeSender() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TripIdPartitioner.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", true);

        SenderOptions<String, Message> options = SenderOptions.<String, Message>create(props)
                .maxInFlight(1024);
        return KafkaSender.create(options);
    }
}
