package com.github.alcereo.kafkatool.sample.consumer;

import com.github.alcereo.kafkatool.KafkaTool;
import com.github.alcereo.kafkatool.consumer.KtConsumerLoop;
import com.github.alcereo.kafkatool.producer.KtProducer;
import com.github.alcereo.kafkatool.topic.KtTopic;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceBusinessStatus;
import processing.DeviceEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@Configuration
public class EventProcessing {

    @Value("${kafka.topic.business-status.name:device-business-status-table}")
    private String DEVICE_BUSINESS_STATUS_TABLE = "device-business-status-table";

    @Value("${kafka.topic.business-status.numparts:20}")
    private Integer businessTopicNumparts = 20;

    @Value("${kafka.topic.event.name:event-topic}")
    private String EVENT_TOPIC = "event-topic";

    @Value("${kafka.topic.event.numparts:20}")
    private Integer eventsTopicNumparts = 20;

    @Value("${kafka.brokers}")
    private String BROKERS = "192.170.0.3:9092";

    @Value("${kafka.registry.url}")
    private String SCHEMA_REGISTRY_URL = "http://192.170.0.6:8081";

    private static final byte[] sessionUUID = UUID.randomUUID().toString().getBytes();

    @Bean
    public KafkaTool kafkaTool(MeterRegistry registry) {
        return KafkaTool.builder()
                .brokers(BROKERS)
                .schemaRegistryUrl(SCHEMA_REGISTRY_URL)
                .meterRegistry(registry)
                .build();
    }

    @Bean
    public KtTopic<Integer, DeviceBusinessStatus> businessStatusTopic(KafkaTool kafkaTool) throws ExecutionException, InterruptedException {
        return kafkaTool
                .topicAvroSimpleTableBuilder(Integer.class, DeviceBusinessStatus.class)
                .topicName(DEVICE_BUSINESS_STATUS_TABLE)
                .numPartitions(businessTopicNumparts)
                .checkOnStartup()
                .build();
    }

    @Bean
    public KtTopic<Integer, DeviceEvent> eventTopic(KafkaTool kafkaTool) throws ExecutionException, InterruptedException {
        return kafkaTool
                .topicAvroSimpleStreamBuilder(Integer.class, DeviceEvent.class)
                .topicName(EVENT_TOPIC)
                .numPartitions(eventsTopicNumparts)
                .checkOnStartup()
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
    public KtProducer<Integer, DeviceBusinessStatus> businessStatusProducer(
            KafkaTool kafkaTool,
            KtTopic<Integer, DeviceBusinessStatus> businessStatusTopic
    ) {
        return kafkaTool.producerKeyPartAppropriatingBuilder(businessStatusTopic)
                        .name("business-status-producer")
                        .build();
    }

    @Bean(destroyMethod = "close")
    public KtConsumerLoop<Integer, DeviceEvent> eventsLoop(
            KafkaTool kafkaTool,
            KtTopic<Integer, DeviceEvent> eventTopic,
            EventInMemoryStore eventWindowStore,
            DeviceBusinessStateInMemoryStore deviceStatusesStore,
            KtProducer<Integer, DeviceBusinessStatus> businessStatusProducer
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
                                        KtProducer.SimpleHeader
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
                                        KtProducer.SimpleHeader
                                                .from("session", sessionUUID)
                                )
                        );

                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), fineStatus);

                    }

                    eventWindowStore.addEvent(deviceEvent);
                }).build();

        eventsConsumerLoop.start();

        return eventsConsumerLoop;
    }

}