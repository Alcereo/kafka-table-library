package com.github.alcereo.kafkatable.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.*;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class Application implements CommandLineRunner {

    static final String DEVICE_BUSINESS_STATUS_TABLE = "device-business-status-table";
    static final Integer NUM_PARTS = 20;
    static final String PARTITIONER_NUMPARTS_PROPERTY_NAME = "device.partitioner.numparts";

    static final String EVENT_TOPIC = "event-topic";


    static final String BROKERS = "192.170.0.3:9092";
    static final String SCHEMA_REGISTRY_URL = "http://192.170.0.6:8081";

    public static void main(String[] args){
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws ExecutionException, InterruptedException {

        log.info("Intent to create topic: "+DEVICE_BUSINESS_STATUS_TABLE);

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        try(AdminClient adminClient = AdminClient.create(config)) {

            Set<String> strings = adminClient.listTopics().names().get();

            if (!strings.contains(DEVICE_BUSINESS_STATUS_TABLE)) {

//        BUSINESS_STATUS_TOPIC ++
                NewTopic businessStatusTopic = new NewTopic(DEVICE_BUSINESS_STATUS_TABLE, NUM_PARTS, (short) 1);
                Map<String, String> properties = new HashMap<>();
                properties.put("cleanup.policy", "compact");
//                properties.put("delete.retention.ms", "20000");
//                properties.put("segment.ms", "20000");
//                properties.put("min.cleanable.dirty.ratio", "0.01");

                businessStatusTopic.configs(properties);

//        BUSINESS_STATUS_TOPIC --

                CreateTopicsResult result = adminClient.createTopics(
                        Collections.singletonList(businessStatusTopic)
                );

                result.all().get();
            }
        }

    }

//    @Bean(destroyMethod = "close")
//    public KafkaStreams kafkaStreams(){
//
//        Properties config = new Properties();
//
//        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "device-events-stream");
//        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
//
//        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
//                SpecificAvroSerde.class);
//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
//                SpecificAvroSerde.class);
//        config.put("schema.registry.url", SCHEMA_REGISTRY_URL);
//
//        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
//
//        final Map<String, String> serdeConfig = Collections.singletonMap(
//                "schema.registry.url",
//                SCHEMA_REGISTRY_URL
//        );
//
//        final Serde<Integer> keySerde = Serdes.Integer();
//        final Serde<DeviceBusinessStatus> valueSpecificAvroSerde = new SpecificAvroSerde<>();
//        valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values
//
//        final Serde<DeviceEvent> deviceEventValueSerde = new SpecificAvroSerde<>();
//        valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values
//
//        ----
//
//        StreamsBuilder builder = new StreamsBuilder();
//
//        KStream<Integer, DeviceEvent> events = builder.stream(EVENT_TOPIC,
//                Consumed.with(keySerde, deviceEventValueSerde));
//
//        KTable<Integer, ProducerRecord<Integer, DeviceBusinessStatus>> reduce = events.filter((key, value) -> value.getEventId().equals("1") | value.getEventId().equals("2"))
//                .map((key, value) -> {
//                    if (value.getEventId().equals("1"))
//                        return new KeyValue<>(key, new ProducerRecord<>(
//                                DEVICE_BUSINESS_STATUS,
//                                value.getDeviceId(),
//                                DeviceBusinessStatus.newBuilder()
//                                        .setStatus("ERROR")
//                                        .build()
//                        ));
//                    else
//                        return new KeyValue<>(key, new ProducerRecord<>(
//                                DEVICE_BUSINESS_STATUS,
//                                value.getDeviceId(),
//                                DeviceBusinessStatus.newBuilder()
//                                        .setStatus("FINE")
//                                        .build()
//                        ));
//                }).groupByKey().reduce((value1, value2) -> value1);
//
//        reduce.
//
//        GlobalKTable<Integer, DeviceBusinessStatus> deviceBusinessStatusTable = builder
//                .globalTable(
//                        DEVICE_BUSINESS_STATUS,
//                        Consumed.with(keySerde, valueSpecificAvroSerde)
//                );
//
//        KafkaStreams streams = new KafkaStreams(builder.build(), config);
//
//        streams.cleanUp();
//        streams.start();
//
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//
//        return streams;
//
//    }

}
