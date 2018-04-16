package com.github.alcereo.kafkatable.producer;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.config.EnableWebFlux;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
@EnableScheduling
@EnableWebFlux
public class Application {

    private static final String TABLE_TOPIC = "longs-table";


    private static final String BROKERS = "broker:9092";
    public static final String TABLE_STORE = "tableStore";


    public static void main(String[] args){
        new SpringApplicationBuilder()
                .web(WebApplicationType.REACTIVE)
                .sources(Application.class)
                .run(args);
    }

    public static void createTopic(AdminClient admin, String topicName) throws ExecutionException, InterruptedException, TimeoutException {

        ListTopicsResult listTopicsResult = admin.listTopics();

        if (!listTopicsResult.names().get().contains(topicName)) {

            Map<String, String> configs = new HashMap<>();
            int partitions = 1;
            short replication = 1;

            CreateTopicsResult topics = admin.createTopics(
                    Collections.singletonList(
                            new NewTopic(topicName, partitions, replication)
                                    .configs(configs)
                    )
            );

            topics.all().get(10, TimeUnit.SECONDS);

        }
    }

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(){
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        return AdminClient.create(config);
    }

    @Bean(destroyMethod = "close")
    public KafkaStreams kafkaStreams(AdminClient client) throws ExecutionException, InterruptedException, TimeoutException {

//        createTopic(client, TABLE_OUT_TOPIC);

//        ---

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-kafka-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.Long().getClass().getName());

        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);

//        ---------------

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Long> longs_table = builder.table(TABLE_TOPIC, Materialized.as(TABLE_STORE));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        return streams;

    }

    @Bean(destroyMethod = "close")
    public KafkaProducer kafkaProducer() {

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.LongSerializer");

        return new KafkaProducer<>(producerConfig);

    }
}
