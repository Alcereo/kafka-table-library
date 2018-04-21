package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.consumer.KtConsumer;
import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@Builder
public class TableSubscriber<K,V> implements Subscriber<K,V> {

    @NonNull
    private Collection<String> topicsCollection;


    @Override
    public void subscribe(KtConsumer<K, V> consumer) {
        final KafkaConsumer<K, V> kafkaConsumer = consumer.getKafkaConsumer();

        kafkaConsumer.subscribe(
                topicsCollection,
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
