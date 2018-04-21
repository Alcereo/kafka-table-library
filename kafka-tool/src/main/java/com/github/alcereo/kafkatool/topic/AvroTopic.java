package com.github.alcereo.kafkatool.topic;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;

public interface AvroTopic<K,V> extends KtTopic<K, V> {

    String getSchemaRegisterUrl();

    @Override
    default String getKeySerializerClassName() {
        return KafkaAvroSerializer.class.getName();
    }

    @Override
    default String getKeyDeserializerClassName() {
        return KafkaAvroDeserializer.class.getName();
    }

    @Override
    default String getValueSerializerClassName() {
        return KafkaAvroSerializer.class.getName();
    }

    @Override
    default String getValueDeserializerClassName() {
        return KafkaAvroDeserializer.class.getName();
    }

    @Override
    default Properties getAdditionalConsumerProperties() {
        Properties properties = new Properties();
        properties.put("schema.registry.url", getSchemaRegisterUrl());
        properties.put("specific.avro.reader", "true");

        return properties;
    }

    @Override
    default Properties getAdditionalProducerProperties() {
        Properties properties = new Properties();
        properties.put("schema.registry.url", getSchemaRegisterUrl());

        return properties;
    }

}
