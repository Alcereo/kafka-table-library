package com.github.alcereo.kafkalibrary;

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

    public <K,V> KafkaConsumerWrapper.Builder<K,V> consumerWrapperBuilder(){
        return new KafkaConsumerWrapper.Builder<>(brokers, schemaRegistryUrl);
    }

}
