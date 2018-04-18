package com.github.alcereo.kafkatable.producer;

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
    private KafkaProducer<Integer, DeviceEvent> kafkaProducer;

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

}
