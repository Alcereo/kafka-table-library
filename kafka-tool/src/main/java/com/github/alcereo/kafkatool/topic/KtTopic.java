package com.github.alcereo.kafkatool.topic;

public interface KtTopic<K,V> {

    /**
     * Send message to topic resp.
     * @param key Message key
     * @param value Message value
     * @return Topic name
     */
    String getTopicName(K key, V value);

    /**
     * Topic creation responsibility
     * @return Config data used for topic creation
     */
    NewTopicConfig getNewTopicConfig();


    Subscriber<K,V> getSubcriber();


    TopicTypeConfig<K,V> getTopicTypeConfig();

}
