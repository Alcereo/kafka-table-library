package com.github.alcereo.kafkatable.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("lines")
public class TestController {

    @Autowired
    private KafkaProducer<String, Long> kafkaProducer;

    @Autowired
    private KafkaStreams kafkaStreams;

    private ExecutorService executor = Executors.newCachedThreadPool();

    @PostMapping
    private Mono<String> addLine(@RequestBody Mono<String> line) throws ExecutionException, InterruptedException {

        return line.map(
                s -> {
                    try {
                        kafkaProducer.send(new ProducerRecord<>(
                                "longs-table", s, new Random().nextLong()
                        )).get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                    return "Success";
                }
        );
    }

    @DeleteMapping
    private void deleteLine(@RequestBody String line) throws ExecutionException, InterruptedException {

        kafkaProducer.send(new ProducerRecord<>(
                "longs-table", line, null
        )).get();

        executor.execute(() -> kafkaProducer.flush());

    }

    @GetMapping
    private Mono<String> getTable(){

        return Mono.fromCallable(() -> {

            ReadOnlyKeyValueStore<String, Long> keyValueStore = kafkaStreams.store(Application.TABLE_STORE, QueryableStoreTypes.keyValueStore());

            StringBuilder builder = new StringBuilder();

            builder.append("=== Publisher store ===").append("\n");

            builder.append("========== Lines =============").append("\n");

            KeyValueIterator<String, Long> all = keyValueStore.all();

            while (all.hasNext()){
                KeyValue<String, Long> next = all.next();
                builder.append("## ").append(next.key).append(" - ").append(next.value).append("\n");
            }

            builder.append("=========== END ==============").append("\n");

            return builder.toString();

        });

    }
}
