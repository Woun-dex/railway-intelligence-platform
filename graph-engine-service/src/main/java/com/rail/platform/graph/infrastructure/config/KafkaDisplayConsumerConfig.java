package com.rail.platform.graph.infrastructure.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rail.platform.schemas.display.DisplayMessage;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import reactor.kafka.receiver.ReceiverOptions;

/**
 * Reactor-Kafka consumer wiring for {@code rail.display}. Pinned to
 * {@link DisplayMessage}; uses a dedicated, unique group so this member always
 * receives every departure message (the SSE bridge fans them out to browsers).
 */
@Configuration
public class KafkaDisplayConsumerConfig {

    @Value("${rail.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${rail.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${rail.kafka.topics.display}")
    private String displayTopic;

    @Bean
    public ReceiverOptions<String, DisplayMessage> displayReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Unique group per instance: the SSE bridge is a broadcast tap, not a worker.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "graph-engine-display-" + java.util.UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, DisplayMessage.class.getName());

        return ReceiverOptions.<String, DisplayMessage>create(props)
                .subscription(List.of(displayTopic));
    }
}
