package com.github.alcereo.kafkatool.topic;

import java.util.Properties;

public interface TopicTypeConfig<K,T> {

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
