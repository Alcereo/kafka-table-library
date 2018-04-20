package com.github.alcereo.kafkatable.consumer;

import com.github.alcereo.kafkalibrary.KafkaConsumerLoop;
import com.github.alcereo.kafkalibrary.KafkaConsumerWrapper;
import com.github.alcereo.kafkalibrary.KafkaProducerWrapper;
import com.github.alcereo.kafkalibrary.KafkaTool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import processing.DeviceBusinessStatus;
import processing.DeviceEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static com.github.alcereo.kafkatable.consumer.Application.*;

@Slf4j
@Configuration
@ComponentScan("com.github.alcereo.kafkatable")
public class EventProcessing {

    private static final byte[] sessionUUID = UUID.randomUUID().toString().getBytes();

    @Bean
    public KafkaTool kafkaTool() {
        return KafkaTool.fromBrokers(BROKERS)
                .schemaRegistry(SCHEMA_REGISTRY_URL);
    }

    @Bean(destroyMethod = "close")
    public KafkaConsumerLoop<Integer, DeviceBusinessStatus> deviceBusinessStatusLoop(
            @Value("${device-state-consumer-group}") String deviceStateConsumerGroup,
            KafkaTool kafkaTool,
            DeviceBusinessStateInMemoryStore deviceStatusesStore
    ) {

        KafkaConsumerWrapper.Builder<Integer, DeviceBusinessStatus> deviceBusinessStatusconsumerBuilder = kafkaTool
                .consumerWrapperBuilder()
                .consumerGroup(deviceStateConsumerGroup)
                .enableAvroSerDe()
                .enableTableSubscription()
                .keyValueClass(Integer.class, DeviceBusinessStatus.class)
                .topic(DEVICE_BUSINESS_STATUS_TABLE);

        KafkaConsumerLoop<Integer, DeviceBusinessStatus> deviceBusinessStatusConsumerLoop = kafkaTool
                .consumerLoopBuilder(deviceBusinessStatusconsumerBuilder)
                .threadsNumber(5)
                .recordFunctionHandling(record -> {
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
                }).build();

        deviceBusinessStatusConsumerLoop.start();

        return deviceBusinessStatusConsumerLoop;
    }

    @Bean(destroyMethod = "close")
    public KafkaProducerWrapper<Integer, DeviceBusinessStatus> businessStatusProducer(
            KafkaTool kafkaTool
    ) {
        return kafkaTool.producerWrapperBuilder()
                        .enableKeyPartAppropriating(NUM_PARTS)
                        .enableAvroSerDe()
                        .keyValueClass(Integer.class, DeviceBusinessStatus.class)
                        .topic(DEVICE_BUSINESS_STATUS_TABLE)
                        .build();
    }

    @Bean(destroyMethod = "close")
    public KafkaConsumerLoop<Integer, DeviceEvent> eventsLoop(
            KafkaTool kafkaTool,
            EventInMemoryStore store,
            DeviceBusinessStateInMemoryStore deviceStatusesStore,
            KafkaProducerWrapper<Integer, DeviceBusinessStatus> businessStatusProducer
    ) {

        KafkaConsumerWrapper.Builder<Integer, DeviceEvent> eventsConsumer = kafkaTool.consumerWrapperBuilder()
                .consumerGroup("event-consumer-1")
                .enableAvroSerDe()
                .keyValueClass(Integer.class, DeviceEvent.class)
                .topic(EVENT_TOPIC);

        KafkaConsumerLoop<Integer, DeviceEvent> eventsConsumerLoop = kafkaTool.consumerLoopBuilder(eventsConsumer)
                .threadsNumber(5)
                .recordFunctionHandling(record -> {
                    DeviceEvent deviceEvent = record.value();

                    if (deviceEvent.getEventId().equals("1")) {

                        DeviceBusinessStatus errorStatus = DeviceBusinessStatus.newBuilder()
                                .setStatus("ERROR")
                                .build();

                        businessStatusProducer.sendSync(
                                deviceEvent.getDeviceId(),
                                errorStatus,
                                Collections.singletonList(
                                        KafkaProducerWrapper.Header
                                                .from("session", sessionUUID)
                                )
                        );

                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), errorStatus);
                    } else if (deviceEvent.getEventId().equals("2")) {

                        DeviceBusinessStatus fineStatus = DeviceBusinessStatus.newBuilder()
                                .setStatus("FINE")
                                .build();

                        businessStatusProducer.sendSync(
                                deviceEvent.getDeviceId(),
                                fineStatus,
                                Collections.singletonList(
                                        KafkaProducerWrapper.Header
                                                .from("session", sessionUUID)
                                )
                        );

                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), fineStatus);
                    }

                    store.addEvent(deviceEvent);
                }).build();

        eventsConsumerLoop.start();

        return eventsConsumerLoop;
    }

}