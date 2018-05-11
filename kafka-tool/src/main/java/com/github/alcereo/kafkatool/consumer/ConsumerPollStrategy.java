package com.github.alcereo.kafkatool.consumer;


public interface ConsumerPollStrategy<K, V> {

    void pollConsumer(KtConsumer<K, V> consumerWrapper, ConsumerRecordHandler<K, V> function) throws Exception;
}
