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

import java.util.*;
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
//            producer.flush();
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
            @NonNull KtTopic<K, V> topic,
            @NonNull KtContext context,
            @NonNull KtPartitioner<K, V> partitioner,
            Integer batchSizeBytes,
            Long bufferMemoryBytes){
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

        //  --- Batch properties ---

        //The producer will attempt to batch records together into fewer requests whenever multiple records are being sent
        // to the same partition. This helps performance on both the client and the server. This configuration controls the
        // default batch size in bytes.
        //No attempt will be made to batch records larger than this size.
        //
        //Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent.
        //
        //A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable
        // batching entirely).
        // A very large batch size may use memory a bit more wastefully as we will always allocate a buffer of the specified
        // batch size in anticipation of additional records.
        producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, Optional.ofNullable(batchSizeBytes).orElse(16384));


        // The total bytes of memory the producer can use to buffer records waiting to be sent to the server.
        // If records are sent faster than they can be delivered to the server the producer will block
        // for max.block.ms after which it will throw an exception.
        //This setting should correspond roughly to the total memory the producer will use, but is not a
        // hard bound since not all memory the producer uses is used for buffering. Some additional memory will be
        // used for compression (if compression is enabled) as well as for maintaining in-flight requests.
        producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Optional.ofNullable(bufferMemoryBytes).orElse(33554432L));


        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 0);


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
        Integer batchSizeBytes;
        private Long bufferMemoryBytes;

        private KtProducerBuilder(KtContext context, KtTopic<K, V> topic, KtPartitioner<K,V> partitioner) {
            this.context = context;
            this.topic = topic;
            this.partitioner = partitioner;
        }

        /**
         * An id string to pass to the server when making requests.
         * The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a
         * logical application name to be included in server-side request logging.
         *
         */
        public KtProducerBuilder<K,V> name(String name){
            this.name = name;
            return this;
        }

        /**
         * The producer will attempt to batch records together into fewer requests whenever multiple records are being
         * sent to the same partition. This helps performance on both the client and the server.
         * This configuration controls the default batch size in bytes.
         * No attempt will be made to batch records larger than this size.
         *
         * Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent.
         *
         * A small batch size will make batching less common and may reduce throughput (a batch size of zero will
         * disable batching entirely). A very large batch size may use memory a bit more wastefully as we will always
         * allocate a buffer of the specified batch size in anticipation of additional records.
         *
         * @param batchSizeBytes batch size in bytes
         */
        public KtProducerBuilder<K,V> batchSizeBytes(Integer batchSizeBytes){
            this.batchSizeBytes = batchSizeBytes;
            return this;
        }


        /**
         * The total bytes of memory the producer can use to buffer records waiting to be sent to the server.
         * If records are sent faster than they can be delivered to the server the producer will block
         * for max.block.ms after which it will throw an exception.
         * This setting should correspond roughly to the total memory the producer will use, but is not a
         * hard bound since not all memory the producer uses is used for buffering. Some additional memory
         * will be used for compression (if compression is enabled) as well as for maintaining in-flight requests.
         * @param bufferMemoryBytes buffer memory in bytes
         */
        public KtProducerBuilder<K,V> bufferMemoryBytes(Long bufferMemoryBytes) {
            this.bufferMemoryBytes = bufferMemoryBytes;
            return this;
        }

        public KtProducer<K,V> build(){
            return KtProducer.buildFrom(
                    name,
                    topic,
                    context,
                    partitioner,
                    batchSizeBytes,
                    bufferMemoryBytes);
        }
    }
}
