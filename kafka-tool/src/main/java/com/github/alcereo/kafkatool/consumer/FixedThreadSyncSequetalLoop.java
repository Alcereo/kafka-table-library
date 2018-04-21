package com.github.alcereo.kafkatool.consumer;

import lombok.Builder;
import lombok.NonNull;

import java.util.Optional;
import java.util.concurrent.Executors;

public class FixedThreadSyncSequetalLoop<K,V> extends KtConsumerLoop<K, V> {

    @Builder(builderClassName = "LoopBuilder")
    public FixedThreadSyncSequetalLoop(Integer threadsNumber,
                                       @NonNull KtConsumer.Builder<K,V> consumerBuilder,
                                       @NonNull ConsumerRecordHandler<K, V> recordHandler) {

        super(
                Executors.newFixedThreadPool(
                        Optional.ofNullable(threadsNumber).orElse(5),
                        new NamedDefaultThreadFactory("kafka-consumer-loop-%d | "+consumerBuilder.getConsumerGroup())),
                consumerBuilder,
                recordHandler,
                SyncSequentalStrategy.<K,V>builder().timeout(300).build(),
                Optional.ofNullable(threadsNumber).orElse(5));
    }

}
