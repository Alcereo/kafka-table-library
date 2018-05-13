package com.github.alcereo.kafkatool.sample.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import processing.DeviceEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class SpringKafkaConfig {

    @Value("${kafka.topic.event.name:event-topic}")
    String EVENT_TOPIC;

    @Value("${kafka.topic.event.numparts:20}")
    Integer NUM_PARTS;

    @Value("${kafka.brokers:}")
    String BROKERS;

    @Value("${kafka.registry.url:}")
    String SCHEMA_REGISTRY_URL;

    @Bean
    public Map<String, Object> producerConfig(){

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        props.put(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                DefaultPartitioner.class.getName()
        );


        if (SCHEMA_REGISTRY_URL.isEmpty()) {
            props.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    IntegerSerializer.class.getName()
            );
            props.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    GenericAvroSerializer.class.getName()
            );

        }else{
            props.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName()
            );
            props.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName()
            );

            props.put(
                    "schema.registry.url",
                    SCHEMA_REGISTRY_URL
            );
        }

        return props;
    }

    @Bean
    public ProducerFactory<Integer, DeviceEvent> producerFactory(
    ){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<Integer, DeviceEvent> eventProducer(
            ProducerFactory<Integer, DeviceEvent> producerFactory
    ){
        KafkaTemplate<Integer, DeviceEvent> integerDeviceEventKafkaTemplate = new KafkaTemplate<>(producerFactory);
        integerDeviceEventKafkaTemplate.setDefaultTopic(EVENT_TOPIC);
        return integerDeviceEventKafkaTemplate;
    }

    public static class GenericAvroSerializer implements Serializer<DeviceEvent> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, DeviceEvent data) {
            try {
                return data.toByteBuffer().array();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
        }
    }
}
