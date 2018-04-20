package com.github.alcereo.kafkalibrary;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceBusinessStatus;

import static com.github.alcereo.kafkalibrary.Application.*;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaTool kafkaTool(){
        return KafkaTool.fromBrokers(BROKERS)
                .schemaRegistry(SCHEMA_REGISTRY_URL);
    }

    @Bean
    public KafkaTopicWrapper<Integer, DeviceBusinessStatus> businessTopic(
            KafkaTool kafkaTool
    ){
        return kafkaTool.topicBuilder()
                .name(DEVICE_BUSINESS_STATUS_TABLE)
                .enableTableSubscription()
                .enableAvroSerDe()
                .keyValueClass(Integer.class, DeviceBusinessStatus.class)
                .build();
    }


    @Bean(destroyMethod = "close")
    public KafkaConsumerLoop<Integer, DeviceBusinessStatus> businessStatusLoop(
            KafkaTool kafkaTool,
            DeviceBusinessStateInMemoryStore store,
            KafkaTopicWrapper<Integer, DeviceBusinessStatus> businessTopic
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
