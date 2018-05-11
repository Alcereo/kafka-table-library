package com.github.alcereo.kafkatool.sample.client;

import com.github.alcereo.kafkatool.KafkaTool;
import com.github.alcereo.kafkatool.consumer.KtConsumerLoop;
import com.github.alcereo.kafkatool.topic.KtTopic;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceBusinessStatus;

import java.util.concurrent.ExecutionException;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.business-status.name:device-business-status-table}")
    private String DEVICE_BUSINESS_STATUS_TABLE;

    @Value("${kafka.topic.business-status.numparts:20}")
    private Integer businessTopicNumparts;

    @Value("${kafka.brokers}")
    private String BROKERS;

    @Value("${kafka.registry.url}")
    private String SCHEMA_REGISTRY_URL;

    @Bean
    public KafkaTool kafkaTool(MeterRegistry registry){
        return KafkaTool
                .builder()
                .brokers(BROKERS)
                .schemaRegistryUrl(SCHEMA_REGISTRY_URL)
                .meterRegistry(registry)
                .build();
    }

    @Bean
    public KtTopic<Integer, DeviceBusinessStatus> businessTopic(
            KafkaTool kafkaTool
    ) throws ExecutionException, InterruptedException {
        return kafkaTool
                .topicAvroSimpleTableBuilder(Integer.class, DeviceBusinessStatus.class)
                .topicName(DEVICE_BUSINESS_STATUS_TABLE)
                .numPartitions(businessTopicNumparts)
                .build();
    }


    @Bean(destroyMethod = "close")
    public KtConsumerLoop<Integer, DeviceBusinessStatus> businessStatusLoop(
            KafkaTool kafkaTool,
            DeviceBusinessStateInMemoryStore store,
            KtTopic<Integer, DeviceBusinessStatus> businessTopic
    ){

        KtConsumerLoop<Integer, DeviceBusinessStatus> consumerLoop = kafkaTool
                .FixedThreadSyncSequetalLoopBuilder(
                        kafkaTool.consumerBuilder(businessTopic)
                                .consumerGroup("device-status-client-1")
                ).threadsNumber(5)
                .recordHandler(store.defaultHandler())
                .build();

        consumerLoop.start();

        return consumerLoop;
    }

}
