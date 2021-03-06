package com.github.alcereo.kafkatool.sample.client;

import com.github.alcereo.kafkatool.consumer.KtStorage;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;

import java.util.HashMap;

@Component
public class DeviceBusinessStateInMemoryStore implements KtStorage<Integer,DeviceBusinessStatus> {

    private HashMap<Integer, DeviceBusinessStatus> statuses = new HashMap<>();

    public synchronized void upsert(Integer deviceId, DeviceBusinessStatus status){
        statuses.put(deviceId, status);
    }

    public HashMap<Integer, DeviceBusinessStatus> getAll(){
        return new HashMap<>(statuses);
    }

}
