package com.github.alcereo.kafkatool.sample.utils.loadgen;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import processing.DeviceEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        count.set(0);

        startLoadgen(loadData.numThread, loadData.url);

        return "Success start";
    }

    @PostMapping("stop")
    public String stopLoad(){
        running = false;
        return "Success stopping";
    }

    private AtomicInteger count = new AtomicInteger();

    private Random random = new Random();

    public void startLoadgen(Integer numThreads, String url) {

        exec.execute(() -> {

            log.info("Start loadgen counting");

            List<Integer> pastRates = new ArrayList<>();

            while (!Thread.currentThread().isInterrupted() && running){

                int countForSecond = count.getAndSet(0);

                pastRates.add(countForSecond);
                double mean = pastRates.stream().collect(Collectors.averagingInt(Integer.class::cast));

                System.out.printf("\rCount for last second: %d. Mean: %5.5f", countForSecond, mean);

                if (pastRates.size()>10){
                    pastRates.clear();
                    System.out.println();
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            log.info("Finish loadgen counting...");

        });

        log.info("Starting loadgen workers. Count: {}", numThreads);

        for (int i = 0; i < numThreads; i++) {

            exec.execute(() -> {
                RestTemplate build = new RestTemplateBuilder()
                        .rootUri(url)
                        .build();

                log.info("Start loadgen");

                while (!Thread.currentThread().isInterrupted() && running) {

                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);

                    generateAndSendEvent(build, headers);
                }

                log.info("Finish loadgen");
            });
        }
    }

    public void generateAndSendEvent(RestTemplate template, HttpHeaders headers){

        count.getAndIncrement();

        DeviceEvent event = DeviceEvent.newBuilder()
                .setDeviceId(random.nextInt(35))
                .setComponentId(String.valueOf(random.nextInt(3)))
                .setEventId(String.valueOf(random.nextInt(3)))
                .setTimestamp(String.valueOf(System.currentTimeMillis()))
                .build();

        HttpEntity<String> entity = new HttpEntity<>(event.toString(), headers);

        template.postForEntity(
                "http://localhost:8080/event",
                entity,
                String.class
        );

    }

}
