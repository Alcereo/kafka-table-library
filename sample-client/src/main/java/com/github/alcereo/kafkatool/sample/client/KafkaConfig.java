package com.github.alcereo.kafkatool.sample.client;

import com.github.alcereo.kafkatool.*;
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
                .build();
    }


    @Bean(destroyMethod = "close")
    public KafkaConsumerLoop<Integer, DeviceBusinessStatus> businessStatusLoop(
            KafkaTool kafkaTool,
            DeviceBusinessStateInMemoryStore store,
            KtTopic<Integer, DeviceBusinessStatus> businessTopic
    ){

        KafkaConsumerWrapper.Builder<Integer, DeviceBusinessStatus> consumerBuilder = kafkaTool.consumerWrapperBuilder()
                .consumerGroup("device-status-client-1")
                .topic(businessTopic);

        KafkaConsumerLoop<Integer, DeviceBusinessStatus> consumerLoop = kafkaTool.consumerLoopBuilder(consumerBuilder)
                .threadsNumber(5)
                .connectStorage(store)
                .build();

        consumerLoop.start();

        return consumerLoop;
    }

}
