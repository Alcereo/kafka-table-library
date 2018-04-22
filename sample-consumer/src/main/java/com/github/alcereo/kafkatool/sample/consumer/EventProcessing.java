package com.github.alcereo.kafkatool.sample.consumer;

import com.github.alcereo.kafkatool.KafkaProducerWrapper;
import com.github.alcereo.kafkatool.KafkaTool;
import com.github.alcereo.kafkatool.consumer.KtConsumerLoop;
import com.github.alcereo.kafkatool.topic.KtTopic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceBusinessStatus;
import processing.DeviceEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static com.github.alcereo.kafkatool.sample.consumer.Application.*;

@Slf4j
@Configuration
public class EventProcessing {

    private static final byte[] sessionUUID = UUID.randomUUID().toString().getBytes();

    @Bean
    public KafkaTool kafkaTool() {
        return KafkaTool.fromBrokers(BROKERS)
                .schemaRegistry(SCHEMA_REGISTRY_URL);
    }

    @Bean
    public KtTopic<Integer, DeviceBusinessStatus> businessStatusTopic(KafkaTool kafkaTool) {
        return kafkaTool
                .topicAvroSimpleTableBuilder(Integer.class, DeviceBusinessStatus.class)
                .topicName(DEVICE_BUSINESS_STATUS_TABLE)
                .numPartitions(NUM_PARTS)
                .build();
    }

    @Bean
    public KtTopic<Integer, DeviceEvent> eventTopic(KafkaTool kafkaTool) {
        return kafkaTool
                .topicAvroSimpleStreamBuilder(Integer.class, DeviceEvent.class)
                .topicName(EVENT_TOPIC)
                .numPartitions(NUM_PARTS)
                .build();
    }

    @Bean(destroyMethod = "close")
    public KtConsumerLoop<Integer, DeviceBusinessStatus> deviceBusinessStatusLoop(
            @Value("${device-state-consumer-group}") String deviceStateConsumerGroup,
            KafkaTool kafkaTool,
            KtTopic<Integer, DeviceBusinessStatus> businessStatusTopic,
            DeviceBusinessStateInMemoryStore deviceStatusesStore
    ) {

        KtConsumerLoop<Integer, DeviceBusinessStatus> deviceBusinessStatusConsumerLoop = kafkaTool
                .FixedThreadSyncSequetalLoopBuilder(
                        kafkaTool.consumerBuilder(businessStatusTopic)
                            .consumerGroup(deviceStateConsumerGroup)
                ).recordHandler(record -> {
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
                })
                .build();

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
    public KtConsumerLoop<Integer, DeviceEvent> eventsLoop(
            KafkaTool kafkaTool,
            KtTopic<Integer, DeviceEvent> eventTopic,
            EventInMemoryStore store,
            DeviceBusinessStateInMemoryStore deviceStatusesStore,
            KafkaProducerWrapper<Integer, DeviceBusinessStatus> businessStatusProducer
    ) {

        KtConsumerLoop<Integer, DeviceEvent> eventsConsumerLoop = kafkaTool
                .FixedThreadSyncSequetalLoopBuilder(
                        kafkaTool
                                .consumerBuilder(eventTopic)
                                .consumerGroup("event-consumer-1")
                ).recordHandler(record -> {
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