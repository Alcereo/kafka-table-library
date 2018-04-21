package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.KtConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public interface TableSubscribe<K,V> extends KtTopic<K,V> {

    Collection<String> getTopicsNames(String consumerGroup);

    @Override
    default void subscribe(KtConsumer<K, V> consumer) {
        final KafkaConsumer<K, V> kafkaConsumer = consumer.getKafkaConsumer();

        kafkaConsumer.subscribe(
                getTopicsNames(consumer.getConsumerGroup()),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        kafkaConsumer.seekToBeginning(partitions);
                    }
                }
        );
    }

}
