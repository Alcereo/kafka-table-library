package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.consumer.KtConsumer;
import lombok.Builder;
import lombok.NonNull;

import java.util.Collection;

@Builder
public class DefaultStreamSubscribe<K,V> implements Subscriber<K,V> {

    @NonNull
    private Collection<String> topicsCollection;

    @Override
    public void subscribe(KtConsumer<K, V> consumer) {
        consumer.getKafkaConsumer().subscribe(topicsCollection);
    }

}
