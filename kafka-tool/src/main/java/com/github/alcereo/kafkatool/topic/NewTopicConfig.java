package com.github.alcereo.kafkatool.topic;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class NewTopicConfig {

    Map<String, String> properties;

    @NonNull
    String name;

    @NonNull
    Integer munPartitions;

    short replicaNumber;

}
