package com.github.alcereo.kafkatable.consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Properties;

@SpringBootApplication
@EnableScheduling
public class Application {

    private static final String TABLE_TOPIC = "longs-table";


    private static final String BROKERS = "192.170.0.3:9092";
    public static final String TABLE_STORE = "tableStore";


    public static void main(String[] args){
        SpringApplication.run(Application.class, args);
    }


    @Bean(destroyMethod = "close")
    public KafkaStreams kafkaStreams(){

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-client-kafka-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.Long().getClass().getName());

        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);

//        ----

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, Long> longs_table = builder
                .globalTable(
                        TABLE_TOPIC,
                        Materialized.as(TABLE_STORE)
                );

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        return streams;

    }

}
