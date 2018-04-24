package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.KtContext;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Builder(builderClassName = "Builder")
@Slf4j
public class AvroSimpleStreamTopic<K,V> implements KtTopic<K,V>{

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
        return DefaultStreamSubscribe.<K,V>builder()
                .topicsCollection(Collections.singleton(topicName))
                .build();
    }

    @Override
    public TopicTypeConfig<K, V> getTopicTypeConfig() {
        return AvroTopicTypeConfig.<K,V>builder()
                .schemaRegistryUrl(schemaRegisterUrl)
                .build();
    }

    //=======================================================================//
    //                              BUILDERS                                 //
    //=======================================================================//

    public static <K,V> AvroSimpleStreamTopic<K,V> buildFrom(
            @NonNull String topicName,
            @NonNull KtContext context,
            @NonNull Integer numPartitions,
            short replicaNumber,
            boolean checkOnStartUp
    ) throws ExecutionException, InterruptedException {

        if (checkOnStartUp){
            checkTopic(topicName, numPartitions, context, replicaNumber);
        }

        return AvroSimpleStreamTopic.<K,V>builder()
                .topicName(topicName)
                .schemaRegisterUrl(context.getSchemaRegistryUrl())
                .numPartitions(numPartitions)
                .replicaNumber(replicaNumber==0?1:replicaNumber)
                .build();
    }

    private static void checkTopic(String topicName, Integer numPartitions, KtContext context, short replicaNumber) throws ExecutionException, InterruptedException {
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

    public static <K,V> KtAvroSimpleStreamTopicBuilder<K,V> ktBuild(
            @NonNull KtContext context
    ){
        return new KtAvroSimpleStreamTopicBuilder<>(context);
    }

    public static class KtAvroSimpleStreamTopicBuilder<K,V>{

        private KtContext context;
        private String topicName;
        private String schemaRegisterUrl;
        private Integer numPartitions;
        private short replicaNumber = 1;
        private boolean checkOnStartUp = false;

        public KtAvroSimpleStreamTopicBuilder(@NonNull KtContext context) {
            this.context = context;
        }

        public KtAvroSimpleStreamTopicBuilder<K,V> topicName(@NonNull String topicName){
            this.topicName = topicName;
            return this;
        }

        public KtAvroSimpleStreamTopicBuilder<K,V> numPartitions(@NonNull Integer numPartitions){
            this.numPartitions = numPartitions;
            return this;
        }

        public KtAvroSimpleStreamTopicBuilder<K,V> replicaNumber(short replicaNumber){
            this.replicaNumber = replicaNumber;
            return this;
        }

        public KtAvroSimpleStreamTopicBuilder<K,V> checkOnStartup(){
            this.checkOnStartUp = true;
            return this;
        }

        public AvroSimpleStreamTopic<K,V> build() throws ExecutionException, InterruptedException {
            return buildFrom(
                    topicName,
                    context,
                    numPartitions,
                    replicaNumber,
                    checkOnStartUp
            );
        }
    }

}
