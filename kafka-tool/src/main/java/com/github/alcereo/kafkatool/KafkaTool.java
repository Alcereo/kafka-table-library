package com.github.alcereo.kafkatool;

import com.github.alcereo.kafkatool.topic.AvroSimpleTableTopic;
import lombok.NonNull;

public class KafkaTool {

    private String brokers;
    private String schemaRegistryUrl;

    private KafkaTool(String brokers) {
        this.brokers = brokers;
    }

//    Public API

    public static KafkaTool fromBrokers(@NonNull String brokers) {
        return new KafkaTool(brokers);
    }

    public KafkaTool schemaRegistry(String url){
        this.schemaRegistryUrl = url;
        return this;
    }

    public <K,V> KafkaProducerWrapper.Builder<K,V> producerWrapperBuilder() {
        return new KafkaProducerWrapper.Builder<>(brokers, schemaRegistryUrl);
    }

    public <K,V> AvroSimpleTableTopic.Builder<K,V> topicAvroSimpleTableBuilder(Class<K> keyClass, Class<V> valueClass){
        return AvroSimpleTableTopic.<K,V>builder().schemaRegisterUrl(schemaRegistryUrl);
    }

    public <K,V> KafkaConsumerWrapper.Builder<K,V> consumerWrapperBuilder(){
        return new KafkaConsumerWrapper.Builder<>(brokers, schemaRegistryUrl);
    }

    public <K, V> KafkaConsumerLoop.Builder<K, V> consumerLoopBuilder(@NonNull KafkaConsumerWrapper.Builder<K,V> consumerBuilder) {
        return new KafkaConsumerLoop.Builder<>(consumerBuilder);
    }
}
