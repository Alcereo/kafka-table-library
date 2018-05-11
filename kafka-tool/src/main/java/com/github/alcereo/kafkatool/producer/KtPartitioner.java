package com.github.alcereo.kafkatool.producer;

import java.util.Properties;

public interface KtPartitioner<K, V> {

    String getPartitionerClassName();

    Properties getProducerAdditionalProperties();

}
