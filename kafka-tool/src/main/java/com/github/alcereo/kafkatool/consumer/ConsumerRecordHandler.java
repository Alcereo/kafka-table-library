package com.github.alcereo.kafkatool.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;

@FunctionalInterface
public interface ConsumerRecordHandler<K,V>{

    void handleRecord(ConsumerRecord<K, V> record) throws Exception;

    default ConsumerRecordHandler<K,V> andThen(ConsumerRecordHandler<K, V> function){
        Objects.requireNonNull(function);
        return record -> {
            handleRecord(record);
            function.handleRecord(record);
        };
    }

}
