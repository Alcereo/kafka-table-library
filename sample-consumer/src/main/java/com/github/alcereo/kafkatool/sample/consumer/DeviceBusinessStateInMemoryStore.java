package com.github.alcereo.kafkatool.sample.consumer;

import com.github.alcereo.kafkatool.consumer.KtStorage;
import lombok.Builder;
import lombok.Value;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DeviceBusinessStateInMemoryStore implements KtStorage<Integer,DeviceBusinessStatus> {

    private ConcurrentHashMap<Integer, StatusRecord> statuses = new ConcurrentHashMap<>(25000, 0.75f,20000);


    public void upsert(Integer deviceId, DeviceBusinessStatus status){
        upsert(deviceId, status, true);
    }

    public void externalUpsert(Integer deviceId, DeviceBusinessStatus status){
        upsert(deviceId, status, false);
    }

    private void upsert(Integer deviceId, DeviceBusinessStatus status, boolean internal){
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
