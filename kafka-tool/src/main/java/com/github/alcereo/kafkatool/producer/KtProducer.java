package com.github.alcereo.kafkatool.producer;

import com.github.alcereo.kafkatool.topic.KtTopic;
import com.github.alcereo.kafkatool.topic.TopicTypeConfig;
import lombok.NonNull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KtProducer<K,V>{
    private KafkaProducer<K,V> producer;
    private KtTopic<K,V> topic;


    private KtProducer(@NonNull KafkaProducer<K, V> producer, @NonNull KtTopic<K,V> topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @lombok.Builder(builderClassName = "Builder")
    private static <K,V> KtProducer<K,V> buildFrom(
            @NonNull KtTopic<K,V> topic,
            @NonNull String brokers,
            @NonNull KtPartitioner<K,V> partitioner
    ){
        Properties producerConfig = new Properties();

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

//        Partitioner
        producerConfig.put(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                partitioner.getPartitionerClassName()
        );
        producerConfig.putAll(partitioner.getProducerAdditionalProperties());

//        Type properties
        TopicTypeConfig<K, V> topicTypeConfig = topic.getTopicTypeConfig();
        producerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                topicTypeConfig.getKeySerializerClassName()
        );
        producerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                topicTypeConfig.getValueSerializerClassName()
        );
        producerConfig.putAll(topicTypeConfig.getAdditionalProducerProperties());


        KafkaProducer<K,V> kafkaProducer = new KafkaProducer<>(producerConfig);

        return new KtProducer<>(kafkaProducer, topic);
    }

    public KafkaProducer<K,V> getProducer(){
        return producer;
    }

    public Future<RecordMetadata> sendAsync(K key, V value){
        return producer.send(
                new ProducerRecord<>(
                        topic.getTopicName(key, value),
                        key,
                        value
                )
        );
    }

    public void sendSync(K key, V value) throws ExecutionException, InterruptedException {
        ProducerRecord<K, V> record = new ProducerRecord<>(
                topic.getTopicName(key, value),
                key,
                value
        );
        syncSendRecord(record);
    }

    public void sendSync(K key, V value, Collection<SimpleHeader> headers) throws ExecutionException, InterruptedException {
        ProducerRecord<K, V> record = new ProducerRecord<>(
                topic.getTopicName(key, value),
                key,
                value
        );
        headers.forEach(header -> record.headers().add(header));
        syncSendRecord(record);
    }

    private void syncSendRecord(ProducerRecord<K, V> record) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> send = producer.send(record);
        producer.flush();
        send.get();
    }


    public void close(){
        producer.close();
    }

    public static class SimpleHeader implements org.apache.kafka.common.header.Header{

        private String key;
        private byte[] value;

        public SimpleHeader(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public static SimpleHeader from(String key, byte[] value){
            return new SimpleHeader(key, value);
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }
    }

//    public static class Builder<K,V>{
//
//        private final String schemaRegistryUrl;
//
//        private Properties producerConfig = new Properties();
//        private String topic;
//
//        Builder(String brokers, String schemaRegistryUrl) {
//            this.schemaRegistryUrl = schemaRegistryUrl;
//            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        }
//
//        public Builder<K,V> enableKeyPartAppropriating(@NonNull Integer partNumber){
//            producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
//                    KeyPartAppropriatePartitioner.class.getName()
//            );
//            producerConfig.put(PARTITIONER_NUMPARTS_PROPERTY_NAME, partNumber);
//            return this;
//        }
//
//        public Builder<K,V> enableAvroSerDe(){
//            Objects.requireNonNull(schemaRegistryUrl,
//                    "Reqired set schemaRegistryUrl from KafkaTool when use Avro.");
//
//            producerConfig.put("schema.registry.url", schemaRegistryUrl);
//            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//                    KafkaAvroSerializer.class.getName());
//            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                    KafkaAvroSerializer.class.getName());
//            return this;
//        }
//
//        @SuppressWarnings("unchecked")
//        public <PK,PV> Builder<PK,PV> keyValueClass(Class<PK> keyClass, Class<PV> valueClass){
//            return (Builder<PK,PV>)this;
//        }
//
//        public Builder<K,V> topic(String topic){
//            this.topic = topic;
//            return this;
//        }
//
//        public KtProducer<K,V> build(){
//            Objects.requireNonNull(topic, "Required set property: topic");
//
//            KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(producerConfig);
//
//            return new KtProducer<>(kvKafkaProducer, topic);
//        }
//
//    }
}
