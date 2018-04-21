package com.github.alcereo.kafkatool.topic;

import lombok.Builder;
import lombok.NonNull;

import java.util.Collections;

@Builder(builderClassName = "Builder")
public class AvroSimpleTableTopic<K,V> implements KtTopic<K,V>{

    @NonNull
    private String topicName;

    @NonNull
    private String schemaRegisterUrl;

    @NonNull
    private Integer numPartitions;

    private short replicaNumber = 1;

    @Override
    public String getTopicName(K key, V value) {
        return topicName;
    }

    @Override
    public NewTopicConfig getNewTopicConfig() {
        return NewTopicConfig.builder()
                .name(topicName)
                .munPartitions(numPartitions)
                .replicaNumber(replicaNumber)
                .build();
    }

    @Override
    public Subscriber<K,V> getSubcriber() {
        return TableSubscriber.<K,V>builder()
                .topicsCollection(Collections.singleton(topicName))
                .build();
    }

    @Override
    public TopicTypeConfig<K, V> getTopicTypeConfig() {
        return AvroTopicTypeConfig.<K,V>builder()
                .schemaRegistryUrl(schemaRegisterUrl)
                .build();
    }

}
