package com.github.alcereo.kafkatool;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Objects;

public class KeyPartAppropriatePartitioner implements Partitioner {

    public static final String PARTITIONER_NUMPARTS_PROPERTY_NAME = "key-part-appr-partitioner.parts-num";

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
