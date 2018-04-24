package com.github.alcereo.kafkatool.topic;

import com.github.alcereo.kafkatool.KtContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AvroSimpleStreamTopic<K,V> extends AbstractAvroSimpleTopic<K,V> {

    private final Subscriber<K, V> subscriber;

    @lombok.Builder(builderClassName = "Builder")
    public AvroSimpleStreamTopic(
            String topicName,
            String schemaRegisterUrl,
            Integer numPartitions,
            short replicaNumber
    ) {
        super(topicName, schemaRegisterUrl, numPartitions, replicaNumber);

        this.subscriber = DefaultStreamSubscribe.<K,V>builder()
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


    public static <K,V> KtAvroSimpleStreamTopicBuilder<K,V> ktBuild(
            @NonNull KtContext context
    ){
        return new KtAvroSimpleStreamTopicBuilder<>(context);
    }

    public static class KtAvroSimpleStreamTopicBuilder<K,V>{

        private KtContext context;
        private String topicName;
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


    private static <K,V> AvroSimpleStreamTopic<K,V> buildFrom(
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
                .replicaNumber(replicaNumber)
                .build();
    }

}
