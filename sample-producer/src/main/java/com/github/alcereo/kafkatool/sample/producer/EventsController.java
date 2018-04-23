package com.github.alcereo.kafkatool.sample.producer;

import com.github.alcereo.kafkatool.producer.KtProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import processing.DeviceEvent;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("event")
public class EventsController {

    @Autowired
    private KtProducer<Integer, DeviceEvent> produer;

    @PostMapping
    private Mono<String> event(@RequestBody Mono<DeviceEvent> eventMono) {

        return eventMono.map(
                event -> {
                    try {
                        produer.sendSync(event.getDeviceId(), event);
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                    return "Success";
                }
        );
    }

}
