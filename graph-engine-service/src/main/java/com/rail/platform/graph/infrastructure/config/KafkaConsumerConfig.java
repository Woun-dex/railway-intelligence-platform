package com.rail.platform.graph.infrastructure.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rail.platform.schemas.telemetry.PositionEvent;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import reactor.kafka.receiver.ReceiverOptions;

/**
 * Reactor-Kafka consumer wiring for the position stream.
 *
 * <p>The value deserializer is the Confluent {@link KafkaProtobufDeserializer},
 * pinned to {@link PositionEvent} via {@code specific.protobuf.value.type} so the
 * Schema-Registry id embedded in each record is resolved into the concrete
 * generated type rather than a {@code DynamicMessage}.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${rail.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${rail.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${rail.kafka.consumer-group}")
    private String consumerGroup;

    @Value("${rail.kafka.topics.position}")
    private String positionTopic;

    @Bean
    public ReceiverOptions<String, PositionEvent> positionReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, PositionEvent.class.getName());

        return ReceiverOptions.<String, PositionEvent>create(props)
                .subscription(List.of(positionTopic));
    }
}
