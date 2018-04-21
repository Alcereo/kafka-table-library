package com.github.alcereo.kafkatool;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

public class DefaultSubscriber implements Subscriber {

    @Override
    public <K, V> void subscribe(KafkaConsumer<K, V> consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }
}
