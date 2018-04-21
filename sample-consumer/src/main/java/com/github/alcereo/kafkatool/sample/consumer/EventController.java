package com.github.alcereo.kafkatool.sample.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import processing.DeviceEvent;

import java.util.List;

@RestController
@RequestMapping("events")
public class EventController {

    @Autowired
    private EventInMemoryStore store;


    @GetMapping
    public String getAll(){
        StringBuilder builder = new StringBuilder();

        builder.append("=== Consumer store ===").append("\n");

        builder.append("========== Last 20 received events =============").append("\n");

        List<DeviceEvent> all = store.getAll();

        all.forEach(event -> builder.append("## ").append(event).append("\n"));

        builder.append("=========== END ==============").append("\n");

        return builder.toString();
    }

}
