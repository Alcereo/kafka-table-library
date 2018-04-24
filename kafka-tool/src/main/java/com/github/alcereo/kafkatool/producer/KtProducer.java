package com.github.alcereo.kafkatool.producer;

import com.github.alcereo.kafkatool.KtContext;
import com.github.alcereo.kafkatool.topic.KtTopic;
import com.github.alcereo.kafkatool.topic.TopicTypeConfig;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class KtProducer<K,V>{

    private String name;
    private KafkaProducer<K,V> producer;
    private KtTopic<K,V> topic;

    private Timer sendingTimer;

    private KtProducer(
            String name,
            @NonNull KafkaProducer<K, V> producer,
            @NonNull KtTopic<K,V> topic,
            Timer sendingTimer)
    {
        this.name           = name;
        this.producer       = producer;
        this.topic          = topic;
        this.sendingTimer   = sendingTimer;
    }

    public KafkaProducer<K,V> getProducer(){
        return producer;
    }

    public Future<RecordMetadata> sendAsync(K key, V value){
        log.trace("Producer: {}, Async sending message: {} - {}", name, key, value);

        return sendingTimer.record(
                () -> producer.send(
                        new ProducerRecord<>(
                                topic.getTopicName(key, value),
                                key,
                                value
                        )
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
        log.trace("Producer: {}, Sync sending message: {}", name, record);

        val timeMeasure = System.nanoTime();
        try {
            Future<RecordMetadata> send = producer.send(record);
            producer.flush();
            send.get();
        }finally {
            val finish = System.nanoTime() - timeMeasure;
            sendingTimer.record(finish, TimeUnit.NANOSECONDS);
        }
    }


    public void close(){
        log.info("Producer: {}, closing the session", name);
        producer.close();
    }

    //=======================================================================//
    //                              CLASSES                                  //
    //=======================================================================//

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

    //=======================================================================//
    //                              BUILDERS                                 //
    //=======================================================================//

    private volatile static AtomicInteger nameMetricCounter = new AtomicInteger(0);

    private static <K,V> KtProducer<K,V> buildFrom(
            String name,
            @NonNull KtTopic<K,V> topic,
            @NonNull KtContext context,
            @NonNull KtPartitioner<K,V> partitioner
    ){
        Properties producerConfig = new Properties();

        String finalName = (name == null ? UUID.randomUUID().toString() : name);

        producerConfig.put("client.id", finalName);

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getBrokers());

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

        Timer timer = context.getMeterRegistry().map(
                meterRegistry -> Timer.builder("kafka-tools.producer." + finalName)
                                    .publishPercentiles(0.5, 0.70, 0.80, 0.95, 0.98, 0.99)
                                    .publishPercentileHistogram()
                                    .register(meterRegistry)
        ).orElse(
                new NoopTimer(new Meter.Id("kafka-tools.producer." + finalName, new ArrayList<>(), null, null, Meter.Type.TIMER))
        );

        return new KtProducer<>(finalName, kafkaProducer, topic, timer);
    }

    public static <K,V> KtProducerBuilder<K,V> ktBuild(KtContext context, KtTopic<K,V> topic, KtPartitioner<K,V> partitioner){
        return new KtProducerBuilder<>(context, topic, partitioner);
    }

    public static class KtProducerBuilder<K,V>{

        private KtContext context;
        private KtTopic<K,V> topic;
        private KtPartitioner<K,V> partitioner;
        String name;

        private KtProducerBuilder(KtContext context, KtTopic<K, V> topic, KtPartitioner<K,V> partitioner) {
            this.context = context;
            this.topic = topic;
            this.partitioner = partitioner;
        }

        public KtProducerBuilder<K,V> name(String name){
            this.name = name;
            return this;
        }

        public KtProducer<K,V> build(){
            return KtProducer.buildFrom(
                    name,
                    topic,
                    context,
                    partitioner
            );
        }
    }
}
