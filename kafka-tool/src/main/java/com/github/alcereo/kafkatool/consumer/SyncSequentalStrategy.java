package com.github.alcereo.kafkatool.consumer;

import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@Builder
public class SyncSequentalStrategy<K, V> implements ConsumerPollStrategy<K, V> {

    @NonNull
    private long timeout;

    @Override
    public void pollConsumer(KtConsumer<K, V> consumerWrapper, ConsumerRecordHandler<K, V> function) throws Exception {
        ConsumerRecords<K, V> consumerRecords = consumerWrapper.pollBlockedWithoutCommit(timeout);

        for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
            function.handleRecord(consumerRecord);
        }

        consumerWrapper.commitSync();
    }

}
