package com.github.alcereo.kafkatable.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import processing.DeviceEvent;

import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
@EnableScheduling
public class Application {

    private static final String TABLE_TOPIC = "longs-table";
    public static final String TABLE_STORE = "tableStore";

    static final String EVENT_TOPIC = "event-avro-topic";


    private static final String BROKERS = "192.170.0.3:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://192.170.0.6:8081";


    public static void main(String[] args){
        SpringApplication.run(Application.class, args);
    }

    @Bean(destroyMethod = "close")
    public KafkaConsumer<String, DeviceEvent> kafkaConsumer(){

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "event-consumer-new-1");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        config.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        config.put("specific.avro.reader", "true");


        KafkaConsumer<String, DeviceEvent> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(Collections.singletonList(EVENT_TOPIC));

        return consumer;
    }

    @Autowired
    KafkaConsumer<String, DeviceEvent> consumer;

    @Autowired
    EventInMemoryStore store;

    @Scheduled(fixedRate = 100)
    public void pollMessages(){

        ConsumerRecords<String, DeviceEvent> records = consumer.poll(300);

        records.forEach(
                record -> {
                    store.addEvent(record.value());
                }
        );

        consumer.commitSync();

    }

//    @Bean(destroyMethod = "close")
//    public KafkaStreams kafkaStreams(){
//
//        Properties config = new Properties();
//
//        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-client-kafka-stream");
//        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
//
//        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
//                Serdes.String().getClass().getName());
//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
//                Serdes.Long().getClass().getName());
//
//        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
//
////        ----
//
//        StreamsBuilder builder = new StreamsBuilder();
//
//        GlobalKTable<String, Long> longs_table = builder
//                .globalTable(
//                        TABLE_TOPIC,
//                        Materialized.as(TABLE_STORE)
//                );
//
//        KafkaStreams streams = new KafkaStreams(builder.build(), config);
//        streams.start();
//
//        return streams;
//
//    }

}
