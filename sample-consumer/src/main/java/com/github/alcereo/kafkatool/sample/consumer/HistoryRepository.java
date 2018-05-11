package com.github.alcereo.kafkatool.sample.consumer;


import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import processing.DeviceEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HistoryRepository {

    private final Counter requestCounter;

    private final ConcurrentHashMap<Integer, HistoryCache> history = new ConcurrentHashMap<>();

    @Autowired
    public HistoryRepository(MeterRegistry registry) {
        requestCounter = Counter.builder("kafka-tools.consumer.history-repository.requests")
//                .publishPercentiles(0.5, 0.70, 0.80, 0.95, 0.98, 0.99)
//                .publishPercentileHistogram()
                .register(registry);
    }

    public List<DeviceEvent> getAllEvents(Integer deviceId, int pageSize, int pageNumber, int limit){
        return Optional.ofNullable(history.get(deviceId))
                .map(historyCache -> historyCache.getEvents(pageSize, pageNumber, limit))
                .orElse(new ArrayList<>());
    }

    public void addDeviceEvent(Integer deviceId, DeviceEvent event){
        history.computeIfAbsent(deviceId, integer -> new HistoryCache())
                .addEvent(event);

        requestCounter.increment();
    }

    public List<Integer> getDevicesId(Integer pageSize, Integer pageNumber, Integer limit) {
        return Optional.ofNullable(
                Lists.partition(
                        history.keySet()
                                .stream()
                                .limit(limit)
                                .collect(Collectors.toList())
                        , pageSize
                ).get(pageNumber)
        ).orElse(new ArrayList<>());
    }


    private static class HistoryCache {
        private final MinMaxPriorityQueue<DeviceEvent> fullHistory =
                MinMaxPriorityQueue
                        .maximumSize(1000)
                        .create();

        private final MinMaxPriorityQueue<DeviceEvent> events = MinMaxPriorityQueue
                .maximumSize(1000)
                .create();


        public List<DeviceEvent> getEvents(int pageSize, int pageNumber, int limit){
            synchronized (events){
                val result = Lists.partition(
                        events.stream()
                                .limit(limit)
                                .collect(Collectors.toList())
                        , pageSize).get(pageNumber);
                return result==null?new ArrayList<>():result;
            }
        }

        public void addEvent(DeviceEvent event){
            synchronized (fullHistory) {
                synchronized (events) {
                    events.add(event);
                    fullHistory.add(event);
                }
            }
        }
    }
}
