package com.github.alcereo.kafkatool;

import com.github.alcereo.kafkatool.consumer.FixedThreadSyncSequetalLoop;
import com.github.alcereo.kafkatool.consumer.KtConsumer;
import com.github.alcereo.kafkatool.producer.KtPartitionerKeyPartAppropriate;
import com.github.alcereo.kafkatool.producer.KtProducer;
import com.github.alcereo.kafkatool.topic.AvroSimpleStreamTopic;
import com.github.alcereo.kafkatool.topic.AvroSimpleTableTopic;
import com.github.alcereo.kafkatool.topic.KtTopic;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Builder;
import lombok.NonNull;

public class KafkaTool {

    private KtContext context;

    public KafkaTool(@NonNull KtContext context) {
        this.context = context;
    }

    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>- <b>brokers</b>
     * <br>
     * <br><b>Optional properties<b>
     * <br>- <b>meterRegistry</b>
     * <br>- <b>schemaRegistryUrl</b>
     *
     * <p></p>
     */
    public static KafkaToolBuilder builder(){
        return new KafkaToolBuilder();
    }

    @Builder
    private static KafkaTool fromContextBuilder(
            @NonNull String brokers,
            MeterRegistry meterRegistry,
            String schemaRegistryUrl
    ){
        KtContext context = KtContext.builder()
                .brokers(brokers)
                .meterRegistry(meterRegistry)
                .schemaRegistryUrl(schemaRegistryUrl)
                .build();

        return new KafkaTool(context);
    }


    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>
     * <br><b>Optional properties<b>
     *
     */
    public <K,V> KtProducer.KtProducerBuilder<K,V> producerKeyPartAppropriatingBuilder(KtTopic<K,V> topic) {

        KtPartitionerKeyPartAppropriate<K, V> partitioner = KtPartitionerKeyPartAppropriate.<K, V>builder()
                .numParts(topic.getNewTopicConfig().getMunPartitions())
                .build();

        return KtProducer.ktBuild(context, topic, partitioner);
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
        return AvroSimpleTableTopic.<K,V>builder().schemaRegisterUrl(context.getSchemaRegistryUrl());
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
        return AvroSimpleStreamTopic.<K,V>builder().schemaRegisterUrl(context.getSchemaRegistryUrl());
    }

    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>- <b>consumerGroup</b>
     * <br>
     * <br><b>Optional properties<b>
     * <br>- <b>maxPollRecords</b> - (default: 500)
     */
    public <K,V> KtConsumer.Builder<K,V> consumerBuilder(KtTopic<K,V> topic){
        return KtConsumer.<K,V>builder().brokers(context.getBrokers()).topic(topic);
    }

    /**
     * <b>REQUIRED PROPERTIES<b>
     * <br>- <b>recordHandler</b>
     * <br>
     * <br><b>Optional properties<b>
     * <br>- <b>threadsNumber</b> - (default: 5)
     */
    public <K, V> FixedThreadSyncSequetalLoop.KtBuilder<K, V> FixedThreadSyncSequetalLoopBuilder(KtConsumer.Builder<K,V> consumerBuilder) {
        return FixedThreadSyncSequetalLoop.ktBuilder(consumerBuilder, context);
    }
}
