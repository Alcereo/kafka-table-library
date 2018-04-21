package com.github.alcereo.kafkatool;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;

public class TableSubscriber implements Subscriber {

    @Override
    public <K, V> void subscribe(KafkaConsumer<K, V> consumer, String topic) {
        consumer.subscribe(
                Collections.singletonList(topic),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        consumer.seekToBeginning(partitions);
                    }
                }
        );
    }
}
