package com.github.alcereo.kafkalibrary;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import processing.DeviceBusinessStatus;

import java.util.HashMap;

@RestController
@RequestMapping("b-status")
public class DeviceBusinessStatusController {

    @Autowired
    private DeviceBusinessStateInMemoryStore store;


    @GetMapping
    public String getAll(){
        StringBuilder builder = new StringBuilder();

        builder.append("============ Client store ===============").append("\n");

        builder.append("========== Devices statuses =============").append("\n");

        HashMap<Integer, DeviceBusinessStatus> all = store.getAll();

        all.forEach( (Integer key, DeviceBusinessStatus status) -> builder
                .append(String.format("=> DeviceId: %3d", key))
                .append("  == Status: ").append(String.format("%10.10s", status.getStatus()))
                .append("\n"));

        builder.append("================= END ===================").append("\n");

        return builder.toString();
    }

}
