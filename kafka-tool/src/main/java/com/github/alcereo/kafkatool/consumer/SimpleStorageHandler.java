package com.github.alcereo.kafkatool.consumer;

import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Builder(builderClassName = "Builder")
public class SimpleStorageHandler<K,V> implements ConsumerRecordHandler<K,V>{

    @NonNull
    KtStorage<K,V> storage;

    @Override
    public void handleRecord(ConsumerRecord<K, V> record) throws Exception {
        storage.upsert(record.key(), record.value());
    }

    public static <K,V> SimpleStorageHandler.Builder<K,V> builderFromStore(KtStorage<K,V> storage) {
        SimpleStorageHandler.Builder<K, V> kvBuilder = new SimpleStorageHandler.Builder<K, V>();
        kvBuilder.storage(storage);
        return kvBuilder;
    }

}
