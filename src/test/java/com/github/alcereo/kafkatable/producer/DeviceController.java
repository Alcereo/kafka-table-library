package com.github.alcereo.kafkatable.producer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import processing.DeviceEvent;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutionException;

import static com.github.alcereo.kafkatable.producer.Application.EVENT_TOPIC;

@RestController
@RequestMapping("event")
public class DeviceController {

    @Autowired
    private KafkaProducer<String, GenericRecord> kafkaProducer;

//    @Autowired
//    private KafkaStreams kafkaStreams;

//    private ExecutorService executor = Executors.newCachedThreadPool();

    @PostMapping
    private Mono<String> event(@RequestBody Mono<DeviceEvent> eventMono) throws ExecutionException, InterruptedException {

        return eventMono.map(
                event -> {
                    try {
                        kafkaProducer.send(
                                new ProducerRecord<>(
                                        EVENT_TOPIC,
                                        event.getDeviceId(),
                                        event
                                )
                        ).get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                    return "Success";
                }
        );
    }

//    @DeleteMapping
//    private void deleteLine(@RequestBody String line) throws ExecutionException, InterruptedException {
//
//        kafkaProducer.send(new ProducerRecord<>(
//                "longs-table", line, null
//        )).get();
//
//        executor.execute(() -> kafkaProducer.flush());
//
//    }

//    @GetMapping
//    private Mono<String> getTable(){
//
//        return Mono.fromCallable(() -> {
//
//            ReadOnlyKeyValueStore<String, Long> keyValueStore = kafkaStreams.store(Application.TABLE_STORE, QueryableStoreTypes.keyValueStore());
//
//            StringBuilder builder = new StringBuilder();
//
//            builder.append("=== Publisher store ===").append("\n");
//
//            builder.append("========== Lines =============").append("\n");
//
//            KeyValueIterator<String, Long> all = keyValueStore.all();
//
//            while (all.hasNext()){
//                KeyValue<String, Long> next = all.next();
//                builder.append("## ").append(next.key).append(" - ").append(next.value).append("\n");
//            }
//
//            builder.append("=========== END ==============").append("\n");
//
//            return builder.toString();
//
//        });
//
//    }

}
