package com.github.alcereo.kafkatool;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.NonNull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.github.alcereo.kafkatool.KeyPartAppropriatePartitioner.PARTITIONER_NUMPARTS_PROPERTY_NAME;

public class KafkaProducerWrapper<K,V>{
    private KafkaProducer<K,V> producer;
    private String topic;

    public KafkaProducerWrapper(@NonNull KafkaProducer<K, V> producer,@NonNull String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public KafkaProducer<K,V> getProducer(){
        return producer;
    }

    public Future<RecordMetadata> sendAsync(K key, V value){
        return producer.send(
                new ProducerRecord<K,V>(
                        topic,
                        key,
                        value
                )
        );
    }

    public void sendSync(K key, V value) throws ExecutionException, InterruptedException {
        ProducerRecord<K, V> record = new ProducerRecord<>(
                topic,
                key,
                value
        );
        syncSendRecord(record);
    }

    public void sendSync(K key, V value, Collection<Header> headers) throws ExecutionException, InterruptedException {
        ProducerRecord<K, V> record = new ProducerRecord<>(
                topic,
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

    public static class Header implements org.apache.kafka.common.header.Header{

        private String key;
        private byte[] value;

        public Header(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public static Header from(String key, byte[] value){
            return new Header(key, value);
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


    public static class Builder<K,V>{

        private final String schemaRegistryUrl;

        private Properties producerConfig = new Properties();
        private String topic;

        Builder(String brokers, String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        }

        public Builder<K,V> enableKeyPartAppropriating(@NonNull Integer partNumber){
            producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    KeyPartAppropriatePartitioner.class.getName()
            );
            producerConfig.put(PARTITIONER_NUMPARTS_PROPERTY_NAME, partNumber);
            return this;
        }

        public Builder<K,V> enableAvroSerDe(){
            Objects.requireNonNull(schemaRegistryUrl,
                    "Reqired set schemaRegistryUrl from KafkaTool when use Avro.");

            producerConfig.put("schema.registry.url", schemaRegistryUrl);
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName());
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    KafkaAvroSerializer.class.getName());
            return this;
        }

        @SuppressWarnings("unchecked")
        public <PK,PV> Builder<PK,PV> keyValueClass(Class<PK> keyClass, Class<PV> valueClass){
            return (Builder<PK,PV>)this;
        }

        public Builder<K,V> topic(String topic){
            this.topic = topic;
            return this;
        }

        public KafkaProducerWrapper<K,V> build(){
            Objects.requireNonNull(topic, "Required set property: topic");

            KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(producerConfig);

            return new KafkaProducerWrapper<>(kvKafkaProducer, topic);
        }

    }
}
