package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.KtConsumer;

import java.util.Properties;

public interface KtTopic<K,V> {

    /**
     * Send message to topic resp.
     * @param key Message key
     * @param value Message value
     * @return Topic name
     */
    String getTopicName(K key, V value);

//    /**
//     * Send message to topic resp.
//     * @param consumerGroup Consumer group name
//     * @return Topic name
//     */
//    String getTopicName(String consumerGroup);

    /**
     * Topic creation responsibility
     * @return Config data used for topic creation
     */
    NewTopicConfig getNewTopicConfig();

    /**
     * Subscribe consumer to topic
     * @param consumer Consumer for subscribe
     */
    void subscribe(KtConsumer<K, V> consumer);


//    Key value types responsibility

    String getKeySerializerClassName();
    String getKeyDeserializerClassName();

    String getValueSerializerClassName();
    String getValueDeserializerClassName();

    default Properties getAdditionalConsumerProperties(){
        return new Properties();
    }

    default Properties getAdditionalProducerProperties(){
        return new Properties();
    }

}
