package com.github.alcereo.kafkatool;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface Subscriber {
    <K, V> void subscribe(KafkaConsumer<K, V> consumer, String topic);
}
