package com.github.alcereo.kafkatool.sample.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.config.EnableWebFlux;

import java.util.*;

@SpringBootApplication
@EnableScheduling
@EnableWebFlux
@Slf4j
public class Application implements CommandLineRunner {

    static final String EVENT_TOPIC = "event-topic";
    static final Integer NUM_PARTS = 20;
    static final String PARTITIONER_NUMPARTS_PROPERTY_NAME = "device.partitioner.numparts";

    private static final String BROKERS = "192.170.0.3:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://192.170.0.6:8081";



    public static void main(String[] args){
        new SpringApplicationBuilder()
                .web(WebApplicationType.REACTIVE)
                .sources(Application.class)
                .run(args);
    }

//    @Bean(destroyMethod = "close")
//    public KafkaStreams kafkaStreams(AdminClient client) throws ExecutionException, InterruptedException, TimeoutException {
//
////        ---
//
//        Properties config = new Properties();
//
//        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-kafka-stream");
//        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
//
//        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
//                Serdes.String().getClass().getName());
//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
//                Serdes.Long().getClass().getName());
//
//        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
//
////        ---------------
//
//        StreamsBuilder builder = new StreamsBuilder();
//
//        KTable<String, Long> longs_table = builder.table(TABLE_TOPIC, Materialized.as(TABLE_STORE));
//
//        KafkaStreams streams = new KafkaStreams(builder.build(), config);
//        streams.start();
//
//        return streams;
//
//    }

    @Bean(destroyMethod = "close")
    public KafkaProducer kafkaProducer() {

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DeviceEventsPartitioner.class.getName());
        producerConfig.put(PARTITIONER_NUMPARTS_PROPERTY_NAME, NUM_PARTS);

        producerConfig.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());

        return new KafkaProducer<>(producerConfig);
    }

    public static class DeviceEventsPartitioner implements Partitioner{

        private int numParts;

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            if (key instanceof Integer){
                return ((Integer) key)%numParts;
            }else
                return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            numParts = Objects.requireNonNull(
                    (Integer) configs.get(PARTITIONER_NUMPARTS_PROPERTY_NAME)
            );
        }
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Intent to create topic: " + EVENT_TOPIC);

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        try(AdminClient adminClient = AdminClient.create(config)) {

            Set<String> strings = adminClient.listTopics().names().get();

            if (!strings.contains(EVENT_TOPIC)) {

//        EVENT_TOPIC ++
                NewTopic topic = new NewTopic(EVENT_TOPIC, NUM_PARTS, (short) 1);
                Map<String, String> properties = new HashMap<>();

                topic.configs(properties);

//        EVENT_TOPIC --

                CreateTopicsResult result = adminClient.createTopics(
                        Collections.singletonList(topic)
                );

                result.all().get();
            }
        }
    }
}
