package com.github.alcereo.kafkatool.sample.producer;

import com.github.alcereo.kafkatool.KafkaTool;
import com.github.alcereo.kafkatool.producer.KtProducer;
import com.github.alcereo.kafkatool.topic.KtTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import processing.DeviceEvent;

import static com.github.alcereo.kafkatool.sample.producer.Application.*;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaTool kafkaTool(){
        return KafkaTool.fromBrokers(BROKERS)
                .schemaRegistry(SCHEMA_REGISTRY_URL);
    }


    @Bean
    public KtTopic<Integer, DeviceEvent> eventTopic(
            KafkaTool kafkaTool
    ){
        return kafkaTool.topicAvroSimpleStreamBuilder(Integer.class, DeviceEvent.class)
                    .topicName(EVENT_TOPIC)
                    .numPartitions(NUM_PARTS)
                    .build();
    }

    @Bean
    public KtProducer<Integer, DeviceEvent> eventProducer(
            KafkaTool kafkaTool,
            KtTopic<Integer, DeviceEvent> eventTopic
    ){
        return kafkaTool.producerKeyPartAppropriatingBuilder(eventTopic)
                    .build();
    }

}
