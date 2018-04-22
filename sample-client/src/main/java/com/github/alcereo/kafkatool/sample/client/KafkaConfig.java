package com.github.alcereo.kafkatool.sample.client;

import com.github.alcereo.kafkatool.KafkaTool;
import com.github.alcereo.kafkatool.consumer.KtConsumerLoop;
import com.github.alcereo.kafkatool.topic.KtTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceBusinessStatus;

import static com.github.alcereo.kafkatool.sample.client.Application.*;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaTool kafkaTool(){
        return KafkaTool.fromBrokers(BROKERS)
                .schemaRegistry(SCHEMA_REGISTRY_URL);
    }

    @Bean
    public KtTopic<Integer, DeviceBusinessStatus> businessTopic(
            KafkaTool kafkaTool
    ){
        return kafkaTool
                .topicAvroSimpleTableBuilder(Integer.class, DeviceBusinessStatus.class)
                .topicName(DEVICE_BUSINESS_STATUS_TABLE)
                .numPartitions(20)
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
