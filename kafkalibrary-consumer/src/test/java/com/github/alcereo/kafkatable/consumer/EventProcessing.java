package com.github.alcereo.kafkatable.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;
import processing.DeviceEvent;

import java.util.*;
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

        int nBusinessThreads = 5;
        deviceBusinessStatePool = Executors.newFixedThreadPool(nBusinessThreads);

        for (int i = 0; i < nBusinessThreads; i++) {
            deviceBusinessStatePool.execute(() -> {

                KafkaConsumer<Integer, DeviceBusinessStatus> consumer = deviceStateConsumer(deviceStateConsumerGroup);

                while (!Thread.currentThread().isInterrupted()){

                    ConsumerRecords<Integer, DeviceBusinessStatus> records = consumer.poll(300);

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

        KafkaProducer<Integer, DeviceBusinessStatus> businessStatusProducer = kafkaProducer();

        for (int i = 0; i < nEventsThreads; i++) {
            eventsProcessingPool.execute(() -> {

                KafkaConsumer<Integer, DeviceEvent> eventsConsumer = eventsConsumer();

                while (!Thread.currentThread().isInterrupted()) {

                    ConsumerRecords<Integer, DeviceEvent> records = eventsConsumer.poll(300);

                    if (!records.isEmpty()) {
                        records.forEach(
                                record -> {
                                    DeviceEvent deviceEvent = record.value();
                                    store.addEvent(deviceEvent);

                                    if (deviceEvent.getEventId().equals("1")) {

                                        DeviceBusinessStatus errorStatus = DeviceBusinessStatus.newBuilder()
                                                .setStatus("ERROR")
                                                .build();

                                        ProducerRecord<Integer, DeviceBusinessStatus> errorRecord = new ProducerRecord<>(
                                                DEVICE_BUSINESS_STATUS_TABLE,
                                                deviceEvent.getDeviceId(),
                                                errorStatus
                                        );

                                        errorRecord.headers().add("session", sessionUUID);
                                        businessStatusProducer.send(errorRecord);
                                        businessStatusProducer.flush();

                                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), errorRecord.value());
                                    } else if (deviceEvent.getEventId().equals("2")) {
                                        ProducerRecord<Integer, DeviceBusinessStatus> fineRecord = new ProducerRecord<>(
                                                DEVICE_BUSINESS_STATUS_TABLE,
                                                deviceEvent.getDeviceId(),
                                                DeviceBusinessStatus.newBuilder()
                                                        .setStatus("FINE")
                                                        .build()
                                        );

                                        fineRecord.headers().add("session", sessionUUID);
                                        businessStatusProducer.send(fineRecord);
                                        businessStatusProducer.flush();

                                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), fineRecord.value());
                                    }
                                }
                        );

                        eventsConsumer.commitSync();
                    }
                }

            });
        }

    }


    public KafkaConsumer<Integer, DeviceEvent> eventsConsumer(){

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "event-consumer-1");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        config.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        config.put("specific.avro.reader", "true");


        KafkaConsumer<Integer, DeviceEvent> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(Collections.singletonList(EVENT_TOPIC));

        return consumer;
    }

    public KafkaProducer<Integer, DeviceBusinessStatus> kafkaProducer() {

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                Application.IntegerKeyDivideDevicePartitioner.class.getName()
        );
        producerConfig.put(PARTITIONER_NUMPARTS_PROPERTY_NAME, NUM_PARTS);

        producerConfig.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());

        return new KafkaProducer<>(producerConfig);
    }


    public KafkaConsumer<Integer, DeviceBusinessStatus> deviceStateConsumer(String deviceStateConsumerGroup){

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, deviceStateConsumerGroup);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        config.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        config.put("specific.avro.reader", "true");


        KafkaConsumer<Integer, DeviceBusinessStatus> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(
                Collections.singletonList(DEVICE_BUSINESS_STATUS_TABLE),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        log.warn("REBALANSING REVOKED!");
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        log.debug("Consumer assigned partitions: "+partitions);
                        consumer.seekToBeginning(partitions);
                    }
                }
        );

        return consumer;
    }

}
