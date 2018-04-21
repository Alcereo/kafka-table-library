package com.github.alcereo.kafkatool.sample.consumer;

import lombok.Builder;
import lombok.Value;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;

import java.util.HashMap;

@Component
public class DeviceBusinessStateInMemoryStore {

    private HashMap<Integer, StatusRecord> statuses = new HashMap<>();


    public void upsert(Integer deviceId, DeviceBusinessStatus status){
        upsert(deviceId, status, true);
    }

    public void externalUpsert(Integer deviceId, DeviceBusinessStatus status){
        upsert(deviceId, status, false);
    }

    private synchronized void upsert(Integer deviceId, DeviceBusinessStatus status, boolean internal){
        statuses.put(
                deviceId,
                StatusRecord.builder()
                        .status(status)
                        .internalChange(internal)
                        .build()
        );
    }

    public HashMap<Integer, StatusRecord> getAll(){
        return new HashMap<>(statuses);
    }

    @Value
    @Builder
    public static class StatusRecord{
        boolean internalChange;
        DeviceBusinessStatus status;
    }

}
