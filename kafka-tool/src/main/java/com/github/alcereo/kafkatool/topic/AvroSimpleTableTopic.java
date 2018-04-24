package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.KtContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AvroSimpleTableTopic<K,V> extends AbstractAvroSimpleTopic<K, V> {

    private final Subscriber<K, V> subscriber;

    @lombok.Builder(builderClassName = "Builder")
    public AvroSimpleTableTopic(
            String topicName,
            String schemaRegisterUrl,
            Integer numPartitions,
            short replicaNumber
    ) {
        super(topicName, schemaRegisterUrl, numPartitions, replicaNumber);

        this.subscriber = TableSubscriber.<K,V>builder()
                .topicsCollection(Collections.singleton(topicName))
                .build();
    }

    @Override
    public Subscriber<K,V> getSubcriber() {
        return subscriber;
    }

    //=======================================================================//
    //                              BUILDERS                                 //
    //=======================================================================//


    public static <K,V> AvroSimpleTableTopic.KtAvroSimpleTableTopicBuilder<K,V> ktBuild(
            @NonNull KtContext context
    ){
        return new AvroSimpleTableTopic.KtAvroSimpleTableTopicBuilder<>(context);
    }

    public static class KtAvroSimpleTableTopicBuilder<K, V> {

        private KtContext context;
        private String topicName;
        private Integer numPartitions;
        private short replicaNumber = 1;
        private boolean checkOnStartUp = false;

        KtAvroSimpleTableTopicBuilder(@NonNull KtContext context) {
            this.context = context;
        }

        public AvroSimpleTableTopic.KtAvroSimpleTableTopicBuilder<K,V> topicName(@NonNull String topicName){
            this.topicName = topicName;
            return this;
        }

        public AvroSimpleTableTopic.KtAvroSimpleTableTopicBuilder<K,V> numPartitions(@NonNull Integer numPartitions){
            this.numPartitions = numPartitions;
            return this;
        }

        public AvroSimpleTableTopic.KtAvroSimpleTableTopicBuilder<K,V> replicaNumber(short replicaNumber){
            this.replicaNumber = replicaNumber;
            return this;
        }

        public AvroSimpleTableTopic.KtAvroSimpleTableTopicBuilder<K,V> checkOnStartup(){
            this.checkOnStartUp = true;
            return this;
        }

        public AvroSimpleTableTopic<K,V> build() throws ExecutionException, InterruptedException {
            return buildFrom(
                    topicName,
                    context,
                    numPartitions,
                    replicaNumber,
                    checkOnStartUp
            );
        }

    }

    private static <K,V> AvroSimpleTableTopic<K,V> buildFrom(
            @NonNull String topicName,
            @NonNull KtContext context,
            @NonNull Integer numPartitions,
            short replicaNumber,
            boolean checkOnStartUp
    ) throws ExecutionException, InterruptedException {

        if (checkOnStartUp){
            checkTopic(topicName, numPartitions, context, replicaNumber);
        }

        return AvroSimpleTableTopic.<K,V>builder()
                .topicName(topicName)
                .schemaRegisterUrl(context.getSchemaRegistryUrl())
                .numPartitions(numPartitions)
                .replicaNumber(replicaNumber==0?1:replicaNumber)
                .build();
    }

}
