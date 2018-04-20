package com.github.alcereo.kafkalibrary;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.Objects;

@AllArgsConstructor
public class KafkaTopicWrapper<K,V> {

    @NonNull @Getter
    private String name;

    @NonNull @Getter
    private boolean enableAvroSerDe;

    @NonNull @Getter
    private Class<K> keyClass;

    @NonNull @Getter
    private Class<V> valueClass;

    @NonNull @Getter
    private boolean tableSubscription;



    public static class Builder<K, V> {

        private String name;

        private boolean tableSubscription = false;
        private boolean enableAvroSerDe = false;
        private Class<K> keyClass;
        private Class<V> valueClass;

        public Builder() {
        }

        public Builder<K, V> name(@NonNull String name){
            this.name = name;
            return this;
        }

        public Builder<K, V> enableAvroSerDe(){
            this.enableAvroSerDe = true;
            return this;
        }

        public Builder<K,V> enableTableSubscription(){
            this.tableSubscription = true;
            return this;
        }

        public <PK,PV> Builder<PK,PV> keyValueClass(Class<PK> keyClass, Class<PV> valueClass){
            Builder<PK,PV> newBuilder = new Builder<>();
            newBuilder.name = name;
            newBuilder.tableSubscription = tableSubscription;
            newBuilder.enableAvroSerDe = enableAvroSerDe;
            newBuilder.keyClass = keyClass;
            newBuilder.valueClass = valueClass;

            return newBuilder;
        }

        public KafkaTopicWrapper<K,V> build(){

            Objects.requireNonNull(name, "Required property: name");
            Objects.requireNonNull(keyClass, "Required property: keyClass");
            Objects.requireNonNull(valueClass, "Required property: valueClass");

            return new KafkaTopicWrapper<>(
                    name,
                    enableAvroSerDe,
                    keyClass,
                    valueClass,
                    tableSubscription
            );
        }

    }
}
