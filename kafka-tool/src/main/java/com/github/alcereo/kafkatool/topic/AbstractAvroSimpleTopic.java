package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.KtContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;
import java.util.concurrent.ExecutionException;


@Slf4j
public abstract class AbstractAvroSimpleTopic<K, V> implements KtTopic<K, V> {

    @NonNull
    private String topicName;

    @NonNull
    private NewTopicConfig newTopicConfig;

    @NonNull
    private TopicTypeConfig<K, V> topicTypeConfig;


    AbstractAvroSimpleTopic(
            @NonNull String topicName,
            @NonNull String schemaRegisterUrl,
            @NonNull Integer numPartitions,
            short replicaNumber
            ) {

        this.topicName = topicName;

        this.newTopicConfig = NewTopicConfig.builder()
                .name(topicName)
                .munPartitions(numPartitions)
                .replicaNumber(replicaNumber)
                .build();

        this.topicTypeConfig = AvroTopicTypeConfig.<K,V>builder()
                .schemaRegistryUrl(schemaRegisterUrl)
                .build();

    }

    @Override
    public String getTopicName(K key, V value) {
        return topicName;
    }

    @Override
    public NewTopicConfig getNewTopicConfig() {
        return newTopicConfig;
    }

    @Override
    public TopicTypeConfig<K, V> getTopicTypeConfig() {
        return topicTypeConfig;
    }

    static void checkTopic(String topicName, Integer numPartitions, KtContext context, short replicaNumber) throws ExecutionException, InterruptedException {
        log.info("Check topic: {}", topicName);

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, context.getBrokers());

        try(AdminClient adminClient = AdminClient.create(config)) {

            Set<String> strings = adminClient.listTopics().names().get();

            if (!strings.contains(topicName)) {
                log.info("Create topic: {}", topicName);

                NewTopic topic = new NewTopic(topicName, numPartitions, replicaNumber);
                Map<String, String> properties = new HashMap<>();

                topic.configs(properties);


                CreateTopicsResult result = adminClient.createTopics(
                        Collections.singletonList(topic)
                );

                result.all().get();

                log.info("Topic successful created: {}", topicName);
            }else {
                log.info("Topic '{}' already exist", topicName);
            }
        }
    }

}
