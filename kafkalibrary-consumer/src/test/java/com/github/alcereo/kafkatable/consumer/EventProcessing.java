package com.github.alcereo.kafkatable.consumer;

import com.github.alcereo.kafkalibrary.KafkaConsumerWrapper;
import com.github.alcereo.kafkalibrary.KafkaProducerWrapper;
import com.github.alcereo.kafkalibrary.KafkaTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;
import processing.DeviceEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.alcereo.kafkatable.consumer.Application.*;

@Component
@Slf4j
public class EventProcessing {

    @Autowired
    EventInMemoryStore store;

    @Autowired
    DeviceBusinessStateInMemoryStore deviceStatusesStore;

    private static final byte[] sessionUUID = UUID.randomUUID().toString().getBytes();
    private final ExecutorService deviceBusinessStatePool;
    private final ExecutorService eventsProcessingPool;

    public EventProcessing(@Value("${device-state-consumer-group}") String deviceStateConsumerGroup) {

        KafkaTool kafkaTool =
                KafkaTool.fromBrokers(BROKERS)
                        .schemaRegistry(SCHEMA_REGISTRY_URL);


        int nBusinessThreads = 5;
        deviceBusinessStatePool = Executors.newFixedThreadPool(nBusinessThreads);

        for (int i = 0; i < nBusinessThreads; i++) {
            deviceBusinessStatePool.execute(() -> {

                KafkaConsumerWrapper<Integer, DeviceBusinessStatus> consumer = kafkaTool.consumerWrapperBuilder()
                        .consumerGroup(deviceStateConsumerGroup)
                        .enableAvroSerDe()
                        .topic(DEVICE_BUSINESS_STATUS_TABLE)
                        .enableTableSubscription()
                        .keyValueClass(Integer.class, DeviceBusinessStatus.class)
                        .build();


                while (!Thread.currentThread().isInterrupted()){

                    ConsumerRecords<Integer, DeviceBusinessStatus> records = consumer.pollWithoutCommit(0);

                    if (!records.isEmpty()) {
                        records.forEach(record -> {
                            log.trace("Don't save record by header");

                            if (record.headers().lastHeader("session") != null) {
                                if (!Arrays.equals(record.headers().lastHeader("session").value(), sessionUUID)) {
                                    deviceStatusesStore.externalUpsert(record.key(), record.value());
                                } else {
                                    log.trace("Don't save record by header");
                                }
                            } else {
                                deviceStatusesStore.externalUpsert(record.key(), record.value());
                            }
                        });

                        consumer.commitSync();
                    }
                }

            });
        }


        int nEventsThreads = 5;
        eventsProcessingPool = Executors.newFixedThreadPool(nEventsThreads);


        KafkaProducerWrapper<Integer, DeviceBusinessStatus> businessStatusProducer =
                kafkaTool.producerWrapperBuilder()
                        .enableKeyPartAppropriating(NUM_PARTS)
                        .enableAvroSerDe()
                        .keyValueClass(Integer.class, DeviceBusinessStatus.class)
                        .topic(DEVICE_BUSINESS_STATUS_TABLE)
                        .build();

        for (int i = 0; i < nEventsThreads; i++) {
            eventsProcessingPool.execute(() -> {

                KafkaConsumerWrapper<Integer, DeviceEvent> eventsConsumer = kafkaTool.consumerWrapperBuilder()
                        .consumerGroup("event-consumer-1")
                        .enableAvroSerDe()
                        .topic(EVENT_TOPIC)
                        .keyValueClass(Integer.class, DeviceEvent.class)
                        .build();

                while (!Thread.currentThread().isInterrupted()) {

                    ConsumerRecords<Integer, DeviceEvent> records = eventsConsumer.pollWithoutCommit(0);

                    if (!records.isEmpty()) {
                        records.forEach(
                                record -> {
                                    DeviceEvent deviceEvent = record.value();
                                    store.addEvent(deviceEvent);

                                    if (deviceEvent.getEventId().equals("1")) {

                                        DeviceBusinessStatus errorStatus = DeviceBusinessStatus.newBuilder()
                                                .setStatus("ERROR")
                                                .build();

                                        try {
                                            businessStatusProducer.sendSync(
                                                    deviceEvent.getDeviceId(),
                                                    errorStatus,
                                                    Collections.singletonList(
                                                            KafkaProducerWrapper.Header
                                                                    .from("session", sessionUUID)
                                                    )
                                            );
                                        } catch (ExecutionException | InterruptedException e) {
                                            e.printStackTrace();
                                        }

                                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), errorStatus);
                                    } else if (deviceEvent.getEventId().equals("2")) {

                                        DeviceBusinessStatus fineStatus = DeviceBusinessStatus.newBuilder()
                                                .setStatus("FINE")
                                                .build();

                                        try {
                                            businessStatusProducer.sendSync(
                                                    deviceEvent.getDeviceId(),
                                                    fineStatus,
                                                    Collections.singletonList(
                                                            KafkaProducerWrapper.Header
                                                                    .from("session", sessionUUID)
                                                    )
                                            );
                                        } catch (ExecutionException | InterruptedException e) {
                                            e.printStackTrace();
                                        }

                                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), fineStatus);
                                    }
                                }
                        );

                        eventsConsumer.commitSync();
                    }
                }

                eventsConsumer.close();

            });
        }

    }


//    public KafkaConsumer<Integer, DeviceEvent> eventsConsumer(){
//
//        Properties config = new Properties();
//        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
//        config.put(ConsumerConfig.GROUP_ID_CONFIG, "event-consumer-1");
//        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//                KafkaAvroDeserializer.class.getName());
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                KafkaAvroDeserializer.class.getName());
//        config.put("schema.registry.url", SCHEMA_REGISTRY_URL);
//        config.put("specific.avro.reader", "true");
//
//
//        KafkaConsumer<Integer, DeviceEvent> consumer = new KafkaConsumer<>(config);
//
//        consumer.subscribe(Collections.singletonList(EVENT_TOPIC));
//
//        return consumer;
//    }
//
//
//    public KafkaConsumer<Integer, DeviceBusinessStatus> deviceStateConsumer(String deviceStateConsumerGroup){
//
//        Properties config = new Properties();
//        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
//        config.put(ConsumerConfig.GROUP_ID_CONFIG, deviceStateConsumerGroup);
//        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//                KafkaAvroDeserializer.class.getName());
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                KafkaAvroDeserializer.class.getName());
//        config.put("schema.registry.url", SCHEMA_REGISTRY_URL);
//        config.put("specific.avro.reader", "true");
//
//
//        KafkaConsumer<Integer, DeviceBusinessStatus> consumer = new KafkaConsumer<>(config);
//
//        consumer.subscribe(
//                Collections.singletonList(DEVICE_BUSINESS_STATUS_TABLE),
//                new ConsumerRebalanceListener() {
//                    @Override
//                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//                        log.warn("REBALANSING REVOKED!");
//                    }
//
//                    @Override
//                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                        log.debug("Consumer assigned partitions: "+partitions);
//                        consumer.seekToBeginning(partitions);
//                    }
//                }
//        );
//
//        return consumer;
//    }

}
