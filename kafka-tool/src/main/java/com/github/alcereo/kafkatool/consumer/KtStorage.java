package com.github.alcereo.kafkatool.consumer;

public interface KtStorage<K,V> {
    void upsert(K key, V value);

    default ConsumerRecordHandler<K,V> defaultHandler(){
        return record -> upsert(record.key(), record.value());
    }
}
