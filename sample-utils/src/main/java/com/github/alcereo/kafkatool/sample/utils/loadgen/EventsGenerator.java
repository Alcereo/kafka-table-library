package com.github.alcereo.kafkatool.sample.utils.loadgen;


import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import processing.DeviceEvent;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("loader")
@Slf4j
public class EventsGenerator {

    private ExecutorService exec = Executors.newCachedThreadPool();
    private volatile boolean running = true;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LoadData{
        Integer numThread;
        String url;
    }

    @PostMapping("start")
    public String startLoad(@RequestBody LoadData loadData){

        log.info("Start load with props: {}", loadData);
        running=true;
//        count.set(0);

        startLoadgen(loadData.numThread, loadData.url);

        return "Success start";
    }

    @PostMapping("stop")
    public String stopLoad(){
        running = false;
        return "Success stopping";
    }

    @Autowired
    public Timer requestTimer;

    @Bean
    public Timer requestTimer(MeterRegistry registry){
        return Timer.builder("loader.timer")
                .publishPercentiles(0.5, 0.8, 0.9, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);
    }

    private Random random = new Random();

    public void startLoadgen(Integer numThreads, String url) {

        log.info("Starting loadgen workers. Count: {}", numThreads);

        for (int i = 0; i < numThreads; i++) {

            exec.execute(() -> {
                RestTemplate build = new RestTemplateBuilder()
                        .build();

                log.info("Start loadgen");

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                while (!Thread.currentThread().isInterrupted() && running) {
                    generateAndSendEvent(build, headers, url);
                }

                log.info("Finish loadgen");
            });
        }
    }

    public void generateAndSendEvent(RestTemplate template, HttpHeaders headers, String url){
        requestTimer.record(() -> {
            DeviceEvent event = DeviceEvent.newBuilder()
                    .setDeviceId(random.nextInt(25000))
                    .setComponentId(String.valueOf(random.nextInt(3)))
                    .setEventId(String.valueOf(random.nextInt(6)))
                    .setTimestamp(String.valueOf(System.currentTimeMillis()))
                    .build();

            HttpEntity<String> entity = new HttpEntity<>(event.toString(), headers);

            template.postForEntity(
                    url,
                    entity,
                    String.class
            );
        });
    }

}
