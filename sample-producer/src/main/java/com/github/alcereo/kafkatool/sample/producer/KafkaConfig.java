package com.github.alcereo.kafkatool.sample.producer;

import com.github.alcereo.kafkatool.KafkaTool;
import com.github.alcereo.kafkatool.producer.KtProducer;
import com.github.alcereo.kafkatool.topic.KtTopic;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceEvent;

import java.util.concurrent.ExecutionException;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.event.name:event-topic}")
    String EVENT_TOPIC;

    @Value("${kafka.topic.event.numparts:20}")
    Integer NUM_PARTS;

    @Value("${kafka.brokers}")
    String BROKERS;

    @Value("${kafka.registry.url}")
    String SCHEMA_REGISTRY_URL;

    @Bean
    public KafkaTool kafkaTool(MeterRegistry registry){
        return KafkaTool.builder()
                .brokers(BROKERS)
                .schemaRegistryUrl(SCHEMA_REGISTRY_URL)
                .meterRegistry(registry)
                .build();
    }


    @Bean
    public KtTopic<Integer, DeviceEvent> eventTopic(
            KafkaTool kafkaTool
    ) throws ExecutionException, InterruptedException {
        return kafkaTool.topicAvroSimpleStreamBuilder(Integer.class, DeviceEvent.class)
                    .topicName(EVENT_TOPIC)
                    .numPartitions(NUM_PARTS)
                    .checkOnStartup()
                    .build();
    }

    @Bean
    public KtProducer<Integer, DeviceEvent> eventProducer(
            KafkaTool kafkaTool,
            KtTopic<Integer, DeviceEvent> eventTopic
    ){
        return kafkaTool.producerKeyPartAppropriatingBuilder(eventTopic)
                .name("event-producer")
//                .batchSizeBytes(16000)
//                .bufferMemoryBytes(500000L)
                .build();
    }

}
