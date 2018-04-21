package com.github.alcereo.kafkatool.sample.consumer;

import com.github.alcereo.kafkatool.sample.consumer.DeviceBusinessStateInMemoryStore.StatusRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
@RequestMapping("b-status")
public class DeviceBusinessStatusController {

    @Autowired
    private DeviceBusinessStateInMemoryStore store;


    @GetMapping
    public String getAll(){
        StringBuilder builder = new StringBuilder();

        builder.append("=== Consumer store ===").append("\n");

        builder.append("========== Devices statuses =============").append("\n");

        HashMap<Integer, StatusRecord> all = store.getAll();

        all.forEach( (key, status) -> builder
                .append("=> DeviceId: ").append(key)
                .append("  == Status: ").append(String.format("%10.10s", status.getStatus().getStatus()))
                .append(" | Internal => ").append(status.isInternalChange())
                .append("\n"));

        builder.append("=========== END ==============").append("\n");

        return builder.toString();
    }

}
