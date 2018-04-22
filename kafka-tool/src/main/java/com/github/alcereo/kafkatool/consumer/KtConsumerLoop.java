package com.github.alcereo.kafkatool.consumer;


import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
//@Builder(buildMethodName = "buildLoop")
public class KtConsumerLoop<K,V> {

    private ExecutorService threadPool;
    private KtConsumer.Builder<K, V> consumerBuilder;
    private final List<KtConsumer<K, V>> consumers = new ArrayList<>();

    @NonNull
    private ConsumerRecordHandler<K, V> recordHandler;

    @NonNull
    private ConsumerPollStrategy<K,V> strategy;

    @Getter
    private volatile boolean active = true;

    @Setter
    private Integer parallelism;


    public KtConsumerLoop(@NonNull ExecutorService threadPool,
                          @NonNull KtConsumer.Builder<K, V> consumerBuilder,
                          @NonNull ConsumerRecordHandler<K, V> recordHandler,
                          @NonNull ConsumerPollStrategy<K,V> strategy,
                          Integer parallelism) {

        this.threadPool = threadPool;
        this.consumerBuilder = consumerBuilder;
        this.recordHandler = recordHandler;
        this.strategy = strategy;
        this.parallelism = Optional.ofNullable(parallelism).orElse(5);
    }

    public void start(){

        for (int i = 0; i < parallelism; i++) {
            threadPool.execute(this::run);
        }
    }

    private synchronized void appendConsumer(KtConsumer<K, V> consumerWrapper){
        consumers.add(consumerWrapper);
    }

    private void run() {

        try (KtConsumer<K, V> consumerWrapper = consumerBuilder.build()) {

            appendConsumer(consumerWrapper);

            while (!Thread.currentThread().isInterrupted() && active) {

                try {
                    strategy.pollConsumer(consumerWrapper, recordHandler);

                } catch (WakeupException e) {
                    log.debug("Consumer wake up in consumer loop");

                } catch (Exception e) {
                    log.error("Exception when handle records.", e);
                }
            }

        }catch (Exception e){
            log.error("Error when build consumer.", e);
        }
    }


    public void close() throws InterruptedException {
        log.debug("Shutdown consumer loop thread pool");

        gotToInactive();
        consumers.forEach(KtConsumer::wakeup);
        threadPool.shutdown();

        if(threadPool.awaitTermination(1, TimeUnit.MINUTES)){
            log.debug("The thread pool "+threadPool+" was terminated graceful");
        }else {
            log.warn("The thread pool "+threadPool+" was terminated with errors.");
        }
    }

    private void gotToInactive() {
        active = false;
    }

//    public static class Builder<K,V>{
//
//        private KtConsumer.Builder<K, V> consumerBuilder;
//        private Integer threadsNumber = 5;
//        private String poolName = "";
//        private ConsumerRecordHandler<K, V> function;
//
//        public Builder(@NonNull KtConsumer.Builder<K,V> consumerBuilder){
//            this.consumerBuilder = consumerBuilder;
//        }
//
//        public Builder<K,V> threadsNumber(@NonNull Integer threadsNumber){
//            this.threadsNumber = threadsNumber;
//            return this;
//        }
//
//        public Builder<K,V> poolName(@NonNull String poolName){
//            this.poolName = poolName;
//            return this;
//        }
//
//        public Builder<K,V> recordFunctionHandling(@NonNull ConsumerRecordHandler<K,V> function){
//            this.function = function;
//            return this;
//        }
//
//        public Builder<K,V> connectStorage(KtStorage<K,V> storage){
//            if (function == null)
//                function = record -> storage.upsert(record.key(), record.value());
//            else
//                function = function.andThen(record -> storage.upsert(record.key(), record.value()));
//
//            return this;
//        }
//
//        public KtConsumerLoop<K, V> build() {
//
//            Objects.requireNonNull(consumerBuilder, "Required property: 'consumerBuilder'");
//            Objects.requireNonNull(function, "Required property: 'recordHandler'");
//
//            if (poolName.isEmpty()) {
//                poolName = "kafka-consumer-loop-%d | " + consumerBuilder.getConsumerGroup();
//            }
//
//            ExecutorService executorService = Executors.newFixedThreadPool(
//                    threadsNumber,
//                    new NamedDefaultThreadPool(poolName)
//            );
//
//            KtConsumerLoop<K,V> consumerLoop = new KtConsumerLoop<>(
//                    executorService,
//                    consumerBuilder,
//                    function
//            );
//
//            consumerLoop.setParallelism(threadsNumber);
//
//            return consumerLoop;
//        }
//
//    }

}
