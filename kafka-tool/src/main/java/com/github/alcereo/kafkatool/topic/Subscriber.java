package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.consumer.KtConsumer;

public interface Subscriber<K,V> {

    /**
     * Subscribe consumer to topic
     * @param consumer Consumer for subscribe
     */
    void subscribe(KtConsumer<K, V> consumer);

}
