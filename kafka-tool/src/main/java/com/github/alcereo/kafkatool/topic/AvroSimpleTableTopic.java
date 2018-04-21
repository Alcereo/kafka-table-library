package com.github.alcereo.kafkatool.topic;

import lombok.Builder;
import lombok.NonNull;

import java.util.Collection;
import java.util.Collections;

@Builder(builderClassName = "Builder")
public class AvroSimpleTableTopic<K,V> implements
        KtTopic<K,V>,
        AvroTopic<K,V>,
        TableSubscribe<K,V> {

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
    public String getSchemaRegisterUrl() {
        return schemaRegisterUrl;
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
    public Collection<String> getTopicsNames(String consumerGroup) {
        return Collections.singletonList(topicName);
    }

//    public static class Builder<K, V> {
//
//        private String name;
//
//        private boolean tableSubscription = false;
//        private boolean enableAvroSerDe = false;
//        private Class<K> keyClass;
//        private Class<V> valueClass;
//
//        public Builder() {
//        }
//
//        public Builder<K, V> name(@NonNull String name){
//            this.name = name;
//            return this;
//        }
//
//        public Builder<K, V> enableAvroSerDe(){
//            this.enableAvroSerDe = true;
//            return this;
//        }
//
//        public Builder<K,V> enableTableSubscription(){
//            this.tableSubscription = true;
//            return this;
//        }
//
//        public <PK,PV> Builder<PK,PV> keyValueClass(Class<PK> keyClass, Class<PV> valueClass){
//            Builder<PK,PV> newBuilder = new Builder<>();
//            newBuilder.name = name;
//            newBuilder.tableSubscription = tableSubscription;
//            newBuilder.enableAvroSerDe = enableAvroSerDe;
//            newBuilder.keyClass = keyClass;
//            newBuilder.valueClass = valueClass;
//
//            return newBuilder;
//        }
//
//        public KafkaTopicWrapper<K,V> build(){
//
//            Objects.requireNonNull(name, "Required property: topicName");
//            Objects.requireNonNull(keyClass, "Required property: keyClass");
//            Objects.requireNonNull(valueClass, "Required property: valueClass");
//
//            return new KafkaTopicWrapper<>(
//                    name,
//                    enableAvroSerDe,
//                    keyClass,
//                    valueClass,
//                    tableSubscription
//            );
//        }
//
//    }
}
