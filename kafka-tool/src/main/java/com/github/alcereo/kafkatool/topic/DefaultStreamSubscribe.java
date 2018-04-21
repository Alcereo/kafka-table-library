package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.KtConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;

public interface DefaultStreamSubscribe<K,V> extends KtTopic<K,V> {

    Collection<String> getTopicsNames(String consumerGroup);

    @Override
    default void subscribe(KtConsumer<K, V> consumer) {
        final KafkaConsumer<K, V> kafkaConsumer = consumer.getKafkaConsumer();

        kafkaConsumer.subscribe(
                getTopicsNames(consumer.getConsumerGroup())
        );
    }

}
