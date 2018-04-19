package com.github.alcereo.kafkatable.consumer;

import lombok.Builder;
import lombok.Value;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;

import java.util.HashMap;

@Component
public class DeviceBusinessStateInMemoryStore {

    private HashMap<Integer, StatusRecord> statuses = new HashMap<>();


    public void upsert(Integer deviceId, DeviceBusinessStatus status){
        statuses.put(
                deviceId,
                StatusRecord.builder()
                        .internalChange(true)
                        .status(status)
                        .build()
        );
    }

    public void externalUpsert(Integer deviceId, DeviceBusinessStatus status){
        statuses.put(
                deviceId,
                StatusRecord.builder()
                        .internalChange(false)
                        .status(status)
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
