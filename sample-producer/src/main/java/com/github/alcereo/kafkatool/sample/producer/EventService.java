package com.github.alcereo.kafkatool.sample.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import processing.DeviceEvent;
import reactor.core.publisher.Mono;

@Service
public class EventService {

    private final KafkaTemplate<Integer, DeviceEvent> producer;

    @Autowired
    public EventService(KafkaTemplate<Integer, DeviceEvent> producer) {
        this.producer = producer;
    }

    public Mono<SendResult<Integer, DeviceEvent>> sendDeviceEvent(DeviceEvent event) {
        return Mono.fromFuture(
                producer.sendDefault(event.getDeviceId(), event).completable()
        );
    }
}
