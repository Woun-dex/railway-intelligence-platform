package com.rail.platform.graph.infrastructure.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.protobuf.Message;
import com.rail.platform.graph.infrastructure.messaging.TripIdPartitioner;
import com.rail.platform.schemas.prediction.CascadeEvent;
import com.rail.platform.schemas.prediction.PredictionEvent;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

/**
 * Kafka wiring for the criticality streaming joiner: two protobuf consumers
 * (cascade + STGCN prediction) feeding the join, and one protobuf producer for
 * the scored {@code rail.criticality.scored} output. Mirrors the existing
 * cascade producer / position consumer configs (Schema-Registry-backed,
 * {@code trip_id} partitioning, idempotent delivery).
 */
@Configuration
public class KafkaCriticalityConfig {

    @Value("${rail.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${rail.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${rail.criticality.consumer-group:graph-criticality-joiner}")
    private String consumerGroup;

    @Value("${rail.kafka.topics.cascade}")
    private String cascadeTopic;

    @Value("${rail.kafka.topics.prediction}")
    private String predictionTopic;

    private Map<String, Object> consumerBaseProps(String groupSuffix, Class<?> valueType) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup + "-" + groupSuffix);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, valueType.getName());
        return props;
    }

    @Bean
    public ReceiverOptions<String, CascadeEvent> cascadeReceiverOptions() {
        return ReceiverOptions.<String, CascadeEvent>create(consumerBaseProps("cascade", CascadeEvent.class))
                .subscription(List.of(cascadeTopic));
    }

    @Bean
    public ReceiverOptions<String, PredictionEvent> predictionReceiverOptions() {
        return ReceiverOptions.<String, PredictionEvent>create(consumerBaseProps("prediction", PredictionEvent.class))
                .subscription(List.of(predictionTopic));
    }

    @Bean
    public KafkaSender<String, Message> criticalitySender() {
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
