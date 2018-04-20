package com.github.alcereo.kafkatool;

import com.github.alcereo.kafkalibrary.KafkaConsumerLoop;
import com.github.alcereo.kafkalibrary.KafkaConsumerWrapper;
import com.github.alcereo.kafkalibrary.KafkaTool;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceBusinessStatus;

import static com.github.alcereo.kafkatool.Application.*;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaTool kafkaTool(){
        return KafkaTool.fromBrokers(BROKERS)
                .schemaRegistry(SCHEMA_REGISTRY_URL);
    }


    @Bean(destroyMethod = "close")
    public KafkaConsumerLoop<Integer, DeviceBusinessStatus> businessStatusLoop(
            KafkaTool kafkaTool,
            DeviceBusinessStateInMemoryStore store
    ){

        KafkaConsumerWrapper.Builder<Integer, DeviceBusinessStatus> consumerBuilder = kafkaTool.consumerWrapperBuilder()
                .consumerGroup("device-status-client-1")
                .enableAvroSerDe()
                .keyValueClass(Integer.class, DeviceBusinessStatus.class)
                .enableTableSubscription()
                .topic(DEVICE_BUSINESS_STATUS_TABLE);

        KafkaConsumerLoop<Integer, DeviceBusinessStatus> consumerLoop = kafkaTool.consumerLoopBuilder(consumerBuilder)
                .threadsNumber(5)
                .recordFunctionHandling(record ->
                        store.upsert(record.key(), record.value())
                ).build();

        consumerLoop.start();

        return consumerLoop;
    }

}
