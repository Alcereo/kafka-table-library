package com.github.alcereo.kafkatable.loadgen;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import processing.DeviceEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
@EnableScheduling
@SpringBootApplication
public class EventsGenerator {

    private final int NUM_TREADS = 20;


    public static void main(String[] args) {
        SpringApplication.run(EventsGenerator.class, args);
    }

    @Bean
    public RestTemplate template() {
        return new RestTemplateBuilder ()
                .build();
    }


    public EventsGenerator() {

        ExecutorService exec = Executors.newCachedThreadPool();

        exec.execute(() -> {

            List<Integer> pastRates = new ArrayList<>();

            while (!Thread.currentThread().isInterrupted()){

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

        });

        for (int i = 0; i < NUM_TREADS; i++) {
            exec.execute(() -> {
                RestTemplate build = new RestTemplateBuilder()
                        .rootUri("http://localhost:8080/")
                        .build();

                while (!Thread.currentThread().isInterrupted()) {

                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);

                    generateAndSendEvent(build, headers);
                }
            });
        }
    }

    @Autowired
    RestTemplate template;

    private AtomicInteger count = new AtomicInteger();

    private Random random = new Random();


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

        try {
            Thread.sleep(10+random.nextInt(100));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
