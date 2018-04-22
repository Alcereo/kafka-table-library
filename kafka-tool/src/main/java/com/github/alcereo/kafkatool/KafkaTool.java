package com.github.alcereo.kafkatool;

import com.github.alcereo.kafkatool.consumer.FixedThreadSyncSequetalLoop;
import com.github.alcereo.kafkatool.consumer.KtConsumer;
import com.github.alcereo.kafkatool.topic.AvroSimpleStreamTopic;
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

    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>- <b>topicName</b>
     * <br>- <b>numPartitions</b>
     * <br>
     * <br><b>Optional properties<b>
     * <br>- <b>replicaNumber</b> - (default: 1)
     *
     */
    public <K,V> AvroSimpleTableTopic.Builder<K,V> topicAvroSimpleTableBuilder(Class<K> keyClass, Class<V> valueClass){
        return AvroSimpleTableTopic.<K,V>builder().schemaRegisterUrl(schemaRegistryUrl);
    }

    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>- <b>topicName</b>
     * <br>- <b>numPartitions</b>
     * <br>
     * <br><b>Optional properties<b>
     * <br>- <b>replicaNumber</b> - (default: 1)
     *
     */
    public <K,V> AvroSimpleStreamTopic.Builder<K,V> topicAvroSimpleStreamBuilder(Class<K> keyClass, Class<V> valueClass){
        return AvroSimpleStreamTopic.<K,V>builder().schemaRegisterUrl(schemaRegistryUrl);
    }

    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>- <b>consumerGroup</b>
     * <br>
     * <br><b>Optional properties<b>
     * <br>- <b>maxPollRecords</b> - (default: 500)
     */
    public <K,V> KtConsumer.Builder<K,V> consumerBuilder(KtTopic<K,V> topic){
        return KtConsumer.<K,V>builder().brokers(brokers).topic(topic);
    }

    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>- <b>recordHandler</b>
     * <br>
     * <br><b>Optional properties<b>
     * <br>- <b>threadsNumber</b> - (default: 5)
     */
    public <K, V> FixedThreadSyncSequetalLoop.LoopBuilder<K, V> FixedThreadSyncSequetalLoopBuilder(KtConsumer.Builder<K,V> consumerBuilder) {
        return FixedThreadSyncSequetalLoop.<K,V>builder().consumerBuilder(consumerBuilder);
    }
}
