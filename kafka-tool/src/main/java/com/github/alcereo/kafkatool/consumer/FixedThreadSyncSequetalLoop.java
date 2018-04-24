package com.github.alcereo.kafkatool.consumer;

import com.github.alcereo.kafkatool.KtContext;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FixedThreadSyncSequetalLoop<K,V> extends KtConsumerLoop<K, V> {

    public FixedThreadSyncSequetalLoop(ExecutorService threadPool, KtConsumer.Builder<K, V> consumerBuilder, ConsumerRecordHandler<K, V> recordHandler, ConsumerPollStrategy<K, V> strategy, Integer parallelism, Timer readingTimer) {
        super(threadPool, consumerBuilder, recordHandler, strategy, parallelism, readingTimer);
    }

    //    --------- Building -----------

    public static <K,V> FixedThreadSyncSequetalLoop<K,V> buildFrom(
            @NonNull KtConsumer.Builder<K,V> consumerBuilder,
            @NonNull ConsumerRecordHandler<K, V> recordHandler,
            @NonNull KtContext context,
            Integer threadsNumber
    ){

        ExecutorService threadPool = Executors.newFixedThreadPool(
                Optional.ofNullable(threadsNumber).orElse(5),
                new NamedDefaultThreadFactory("kafka-consumer-loop-%d | "+consumerBuilder.getConsumerGroup()));

        ConsumerPollStrategy<K, V> strategy = SyncSequentalStrategy.<K,V>builder().timeout(300).build();

        Integer parallelism = Optional.ofNullable(threadsNumber).orElse(5);

        Timer readingTimer = context.getMeterRegistry().map(
                meterRegistry -> Timer.builder("kafka-tools.consumer." + consumerBuilder.getConsumerGroup())
                        .publishPercentiles(0.5, 0.70, 0.80, 0.95, 0.98, 0.99)
                        .publishPercentileHistogram()
                        .register(meterRegistry)
        ).orElse(
                new NoopTimer(new Meter.Id("kafka-tools.producer."+consumerBuilder.getConsumerGroup(), new ArrayList<>(), null, null, Meter.Type.TIMER))
        );

        return new FixedThreadSyncSequetalLoop<>(
                threadPool,
                consumerBuilder,
                recordHandler,
                strategy,
                parallelism,
                readingTimer
        );
    }

    public static <K,V> KtBuilder<K,V> ktBuilder(
            @NonNull KtConsumer.Builder<K, V> consumerBuilder,
            @NonNull KtContext context)
    {
        return new KtBuilder<>(consumerBuilder, context);
    }

    public static class KtBuilder<K,V>{

        KtConsumer.Builder<K,V> consumerBuilder;
        KtContext context;
        ConsumerRecordHandler<K, V> recordHandler;
        Integer threadsNumber;

        public KtBuilder(KtConsumer.Builder<K, V> consumerBuilder, KtContext context) {
            this.consumerBuilder = consumerBuilder;
            this.context = context;
        }

        public KtBuilder<K,V> recordHandler(ConsumerRecordHandler<K, V> recordHandler){
            this.recordHandler = recordHandler;
            return this;
        }

        public KtBuilder<K,V> threadsNumber(Integer threadsNumber){
            this.threadsNumber = threadsNumber;
            return this;
        }

        public FixedThreadSyncSequetalLoop<K,V> build(){
            return buildFrom(
                    consumerBuilder,
                    recordHandler,
                    context,
                    threadsNumber
            );
        }
    }

}
