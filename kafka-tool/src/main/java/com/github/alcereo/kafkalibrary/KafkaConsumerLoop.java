package com.github.alcereo.kafkalibrary;


import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class KafkaConsumerLoop<K,V> {

    private ExecutorService threadPool;
    private KafkaConsumerWrapper.Builder<K, V> consumerBuilder;
    private List<KafkaConsumerWrapper<K, V>> consumers = new ArrayList<>();

    private RecordHandlingFunction<K, V> function;

    private volatile boolean active = true;


    @Getter @Setter
    private Integer parallelism = 5;

    public KafkaConsumerLoop(@NonNull ExecutorService threadPool,
                             @NonNull KafkaConsumerWrapper.Builder<K, V> consumerBuilder,
                             @NonNull RecordHandlingFunction<K, V> function) {

        this.threadPool = threadPool;
        this.consumerBuilder = consumerBuilder;
        this.function = function;
    }

    public void start(){

        for (int i = 0; i < parallelism; i++) {
            threadPool.execute(this::run);
        }
    }

    private synchronized void appendConsumer(KafkaConsumerWrapper<K, V> consumerWrapper){
        consumers.add(consumerWrapper);
    }

    private void run() {

        try (KafkaConsumerWrapper<K, V> consumerWrapper = consumerBuilder.build()) {

            appendConsumer(consumerWrapper);

            while (!Thread.currentThread().isInterrupted() && active) {

                try {
                    ConsumerRecords<K, V> consumerRecords = consumerWrapper.pollBlockedWithoutCommit(300);

//                Придется обрабатывать по порядку. Нам важна гарантия последовательности.
                    for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                        function.handleRecord(consumerRecord);
                    }

                    consumerWrapper.commitSync();
                } catch (WakeupException e) {
                    log.debug("Wakeup consumer in group: " + consumerBuilder.getConsumerGroup());

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
        consumers.forEach(KafkaConsumerWrapper::wakeup);
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

    public static class Builder<K,V>{

        private KafkaConsumerWrapper.Builder<K, V> consumerBuilder;
        private Integer threadsNumber = 5;
        private String poolName = "";
        private RecordHandlingFunction<K, V> function;

        public Builder(@NonNull KafkaConsumerWrapper.Builder<K,V> consumerBuilder){
            this.consumerBuilder = consumerBuilder;
        }

        public Builder<K,V> threadsNumber(@NonNull Integer threadsNumber){
            this.threadsNumber = threadsNumber;
            return this;
        }

        public Builder<K,V> poolName(@NonNull String poolName){
            this.poolName = poolName;
            return this;
        }

        public Builder<K,V> recordFunctionHandling(@NonNull RecordHandlingFunction<K,V> function){
            this.function = function;
            return this;
        }

        public Builder<K,V> connectStorage(KTStorage<K,V> storage){
            if (function == null)
                function = record -> storage.upsert(record.key(), record.value());
            else
                function = function.andThen(record -> storage.upsert(record.key(), record.value()));

            return this;
        }

        public KafkaConsumerLoop<K, V> build() {

            Objects.requireNonNull(consumerBuilder, "Required property: 'consumerBuilder'");
            Objects.requireNonNull(function, "Required property: 'function'");

            if (poolName.isEmpty()) {
                poolName = "kafka-consumer-loop-%d | " + consumerBuilder.getConsumerGroup();
            }

            ExecutorService executorService = Executors.newFixedThreadPool(
                    threadsNumber,
                    new NamedDefaultThreadPool(poolName)
            );

            KafkaConsumerLoop<K,V> consumerLoop = new KafkaConsumerLoop<>(
                    executorService,
                    consumerBuilder,
                    function
            );

            consumerLoop.setParallelism(threadsNumber);

            return consumerLoop;
        }

    }

    @FunctionalInterface
    public interface RecordHandlingFunction<K,V>{
        void handleRecord(ConsumerRecord<K,V> record) throws Exception;

        default RecordHandlingFunction<K,V> andThen(RecordHandlingFunction<K,V> function){
            Objects.requireNonNull(function);
            return record -> {
                handleRecord(record);
                function.handleRecord(record);
            };
        }
    }

    static class NamedDefaultThreadPool implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedDefaultThreadPool(String poolName) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = String.format(poolName + " | thread-", poolNumber.getAndIncrement());
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

}
