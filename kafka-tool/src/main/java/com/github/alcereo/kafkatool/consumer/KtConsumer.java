package com.github.alcereo.kafkatool.consumer;

import com.github.alcereo.kafkatool.topic.KtTopic;
import com.github.alcereo.kafkatool.topic.TopicTypeConfig;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Optional;
import java.util.Properties;


public class KtConsumer<K,V> implements AutoCloseable{

    private KafkaConsumer<K,V> consumer;

    @NonNull
    private String consumerGroup;

    @lombok.Builder(builderClassName = "Builder")
    public KtConsumer(@NonNull String consumerGroup,
                      @NonNull KtTopic<K,V> topic,
                      @NonNull String brokers,
                      Integer maxPollRecords
    ) {
        Properties config = new Properties();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Optional.ofNullable(maxPollRecords).orElse(50000));

        TopicTypeConfig<K, V> topicTypeConfig = topic.getTopicTypeConfig();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, topicTypeConfig.getKeyDeserializerClassName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, topicTypeConfig.getValueDeserializerClassName());
        config.putAll(topicTypeConfig.getAdditionalConsumerProperties());

        this.consumer = new KafkaConsumer<>(config);

        topic.getSubcriber().subscribe(this);
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public KafkaConsumer<K, V> getKafkaConsumer() {
        return consumer;
    }


    public ConsumerRecords<K, V> pollBlockedWithoutCommit(long timeout) throws WakeupException {
        return consumer.poll(timeout);
    }


    public void commitSync() {
        consumer.commitSync();
    }

    @Override
    public void close() {
        consumer.close();
    }


    public void wakeup(){
        consumer.wakeup();
    }

    public static class Builder<K,V>{
        @NonNull @Getter
        private String consumerGroup;

    }

//    public static class Builder<K,V>{

//        private final String schemaRegistryUrl;

//        private Properties config = new Properties();
//        private String topic;

//        @Getter
//        private String consumerGroup;
//        private Subscriber subscriber = new DefaultSubscriber();

//        Builder(String brokers, String schemaRegistryUrl) {
//            this.schemaRegistryUrl = schemaRegistryUrl;
//            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//
//            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//            config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        }

//        public Builder<K,V> setMaxPollRecords(@NonNull Integer num){
//            config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(num));
//            return this;
//        }

//        public Builder<K,V> consumerGroup(String consumerGroup){
//            config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
//            this.consumerGroup = consumerGroup;
//            return this;
//        }

//        public Builder<K,V> enableTableSubscription(){
//            this.subscriber = new TableSubscriber();
//            return this;
//        }

//        public Builder<K,V> enableAvroSerDe(){
//            Objects.requireNonNull(schemaRegistryUrl,
//                    "Reqired set schemaRegistryUrl from KafkaTool when use Avro.");
//
//            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//                    KafkaAvroDeserializer.class.getName());
//            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                    KafkaAvroDeserializer.class.getName());
//            config.put("schema.registry.url", schemaRegistryUrl);
//            config.put("specific.avro.reader", "true");
//            return this;
//        }

//        @SuppressWarnings("unchecked")
//        public <PK,PV> Builder<PK,PV> keyValueClass(Class<PK> keyClass, Class<PV> valueClass){
//            return (KtConsumer.Builder<PK,PV>)this;
//        }

//        public Builder<K,V> topic(String topic){
//            this.topic = topic;
//            return this;
//        }

//        @SuppressWarnings("unchecked")
//        public <PK,PV> Builder<PK,PV> topic(KafkaTopicWrapper<PK,PV> topic){
//
//            Builder<PK,PV> result = keyValueClass(topic.getKeyClass(), topic.getValueClass())
//                    .topic(topic.getName());
//
//            if (topic.isEnableAvroSerDe()){
//                result.enableAvroSerDe();
//            }
//
//            if (topic.isTableSubscription()){
//                result.enableTableSubscription();
//            }
//
//            return result;
//        }

//        public KtConsumer<K,V> build(){
//
//            Objects.requireNonNull(topic,"Required set property: topic");
//            Objects.requireNonNull(consumerGroup,"Required set property: consumerGroup");
//
//            KafkaConsumer<K,V> consumer = new KafkaConsumer<>(config);
//            subscriber.subscribe(consumer, topic);
//
//            return new KtConsumer<>(consumer);
//        }


//    }

}
