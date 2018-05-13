package com.github.alcereo.kafkatool.sample.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import processing.DeviceEvent;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("event")
public class EventsController {

    @Autowired
    private EventService service;

    @PostMapping
    private Mono<String> event(@RequestBody DeviceEvent event) {

        return service.sendDeviceEvent(event)
                .doOnError(throwable -> {
                    throw new RuntimeException(throwable);
                }).map(integerDeviceEventSendResult -> "success");
    }

}
