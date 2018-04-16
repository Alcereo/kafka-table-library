package com.github.alcereo.kafkatable.consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Properties;

@SpringBootApplication
@EnableScheduling
public class Application {

    private static final String TABLE_TOPIC = "longs-table";


    private static final String BROKERS = "broker:9092";
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

//        longs_table.toStream().foreach((key, value) -> {
//            lineService.upsertLine(
//                    key,
//                    Optional.ofNullable(value)
//            );
//        });

//        KStream<String, Long> stringLongKStream = longs_table.toStream();
//        stringLongKStream.to(TABLE_OUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        return streams;

    }


//    @Bean(destroyMethod = "close")
//    public KafkaConsumer<String, Long> kafkaConsumer(){
//
//        Properties config = new Properties();
//        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
//        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
//        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
//        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "200");
//
//
//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//                StringDeserializer.class.getName()
//        );
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                LongDeserializer.class.getName()
//        );
//
//        KafkaConsumer<String, Long> stringLongKafkaConsumer = new KafkaConsumer<>(config);
//
//        stringLongKafkaConsumer.subscribe(Collections.singletonList(TABLE_OUT_TOPIC));
//
//        return stringLongKafkaConsumer;
//    }

//    @Autowired
//    private KafkaConsumer<String, Long> kafkaConsumer;

    @Autowired
    private LinesService lineService;

//    @Scheduled(fixedRate = 200)
//    public void consumerScheduler(){
//        ConsumerRecords<String, Long> records = kafkaConsumer.poll(100);
//
//        records.forEach(stringLongConsumerRecord -> {
//            lineService.upsertLine(
//                    stringLongConsumerRecord.key(),
//                    Optional.ofNullable(stringLongConsumerRecord.value())
//            );
//        });
//
//        kafkaConsumer.commitSync();
//    }

}
