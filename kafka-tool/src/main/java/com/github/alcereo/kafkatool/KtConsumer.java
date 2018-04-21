package com.github.alcereo.kafkatool;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KtConsumer<K,V> extends AutoCloseable{

    KafkaConsumer<K,V> getKafkaConsumer();

    ConsumerRecords<K, V> pollBlockedWithoutCommit(long timeout);

    void commitSync();

    void wakeup();

    String getConsumerGroup();
}
