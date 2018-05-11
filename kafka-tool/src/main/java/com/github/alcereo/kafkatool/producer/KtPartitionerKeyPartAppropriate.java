package com.github.alcereo.kafkatool.producer;

import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;


public class KtPartitionerKeyPartAppropriate<K,V> implements KtPartitioner<K,V>{

    public static final String PARTITIONER_NUMPARTS_PROPERTY_NAME = "key-part-appr-partitioner.parts-num";
    private Integer numParts;

    @Builder
    public KtPartitionerKeyPartAppropriate(@NonNull Integer numParts) {
        this.numParts = numParts;
    }

    @Override
    public String getPartitionerClassName() {
        return KeyPartAppropriatePartitioner.class.getName();
    }

    @Override
    public Properties getProducerAdditionalProperties() {
        Properties properties = new Properties();
        properties.put(PARTITIONER_NUMPARTS_PROPERTY_NAME, numParts);
        return properties;
    }

    public static class KeyPartAppropriatePartitioner implements Partitioner{

        private int numParts;

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            if (key instanceof Integer){
                return ((Integer) key)%numParts;
            }else
                return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            numParts = Objects.requireNonNull(
                    (Integer) configs.get(PARTITIONER_NUMPARTS_PROPERTY_NAME)
            );
        }

    }

}


