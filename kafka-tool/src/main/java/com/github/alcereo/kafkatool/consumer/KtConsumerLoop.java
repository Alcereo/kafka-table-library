package com.github.alcereo.kafkatool.consumer;


import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
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

    private Timer readingTimer;


    public KtConsumerLoop(@NonNull ExecutorService threadPool,
                          @NonNull KtConsumer.Builder<K, V> consumerBuilder,
                          @NonNull ConsumerRecordHandler<K, V> recordHandler,
                          @NonNull ConsumerPollStrategy<K,V> strategy,
                          Integer parallelism,
                          @NonNull Timer readingTimer) {

        this.threadPool = threadPool;
        this.consumerBuilder = consumerBuilder;
        this.recordHandler = recordHandler;
        this.strategy = strategy;
        this.parallelism = Optional.ofNullable(parallelism).orElse(5);
        this.readingTimer = readingTimer;
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

                val timeMeasure = System.nanoTime();
                try {
                    strategy.pollConsumer(consumerWrapper, recordHandler);

                } catch (WakeupException e) {
                    log.debug("Consumer wake up in consumer loop");

                } catch (Exception e) {
                    log.error("Exception when handle records.", e);
                }finally {
                    val finish = System.nanoTime() - timeMeasure;
                    readingTimer.record(finish, TimeUnit.NANOSECONDS);
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

}
