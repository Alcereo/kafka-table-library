package com.github.alcereo.kafkatool;

import com.github.alcereo.kafkatool.consumer.FixedThreadSyncSequetalLoop;
import com.github.alcereo.kafkatool.consumer.KtConsumer;
import com.github.alcereo.kafkatool.topic.AvroSimpleTableTopic;
import com.github.alcereo.kafkatool.topic.KtTopic;
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

    public <K,V> KtConsumer.Builder<K,V> consumerBuilder(KtTopic<K,V> topic){
        return KtConsumer.<K,V>builder().brokers(brokers).topic(topic);
    }

    public <K, V> FixedThreadSyncSequetalLoop.LoopBuilder<K, V> FixedThreadSyncSequetalLoopBuilder(KtConsumer.Builder<K,V> consumerBuilder) {
        return FixedThreadSyncSequetalLoop.<K,V>builder().consumerBuilder(consumerBuilder);
    }
}
