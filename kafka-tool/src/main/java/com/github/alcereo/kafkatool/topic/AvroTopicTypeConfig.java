package com.github.alcereo.kafkatool.topic;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Builder;
import lombok.NonNull;

import java.util.Properties;

@Builder
public class AvroTopicTypeConfig<K,V> implements TopicTypeConfig<K, V> {

    @NonNull
    private String schemaRegistryUrl;

    @Override
    public String getKeySerializerClassName() {
        return KafkaAvroSerializer.class.getName();
    }

    @Override
    public String getKeyDeserializerClassName() {
        return KafkaAvroDeserializer.class.getName();
    }

    @Override
    public String getValueSerializerClassName() {
        return KafkaAvroSerializer.class.getName();
    }

    @Override
    public String getValueDeserializerClassName() {
        return KafkaAvroDeserializer.class.getName();
    }

    @Override
    public Properties getAdditionalConsumerProperties() {
        Properties properties = new Properties();
        properties.put("schema.registry.url", schemaRegistryUrl);
        properties.put("specific.avro.reader", "true");

        return properties;
    }

    @Override
    public Properties getAdditionalProducerProperties() {
        Properties properties = new Properties();
        properties.put("schema.registry.url", schemaRegistryUrl);

        return properties;
    }

}
