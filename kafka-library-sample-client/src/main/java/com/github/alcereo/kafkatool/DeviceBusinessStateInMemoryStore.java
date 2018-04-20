package com.github.alcereo.kafkatool;

import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;

import java.util.HashMap;

@Component
public class DeviceBusinessStateInMemoryStore {

    private HashMap<Integer, DeviceBusinessStatus> statuses = new HashMap<>();


    public synchronized void upsert(Integer deviceId, DeviceBusinessStatus status){
        statuses.put(deviceId, status);
    }

    public HashMap<Integer, DeviceBusinessStatus> getAll(){
        return new HashMap<>(statuses);
    }

}
