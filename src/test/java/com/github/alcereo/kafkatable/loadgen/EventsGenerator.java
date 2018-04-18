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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import processing.DeviceEvent;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@EnableScheduling
@SpringBootApplication
public class EventsGenerator {

    public static void main(String[] args) {
        SpringApplication.run(EventsGenerator.class, args);
    }

    @Bean
    public RestTemplate template() {
        return new RestTemplateBuilder ()
                .rootUri("http://localhost:8080/")
                .build();
    }


    public EventsGenerator() {

        ExecutorService exec = Executors.newCachedThreadPool();

        for (int i = 0; i < 5; i++) {
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


//    @Bean
//    public Timer requestTimer(MeterRegistry registry){
//        return Timer
//                .builder("line.request.timer")
//                .register(registry);
//    }
//
//
//    @Bean
//    public Gauge requestSpeedCounter(MeterRegistry registry){
//        return Gauge.builder(requestSpeed, 0d, value -> value)
//                .register(registry);
//    }
//
//    @Autowired
//    Gauge requestSpeedCounter;
//
//    @Autowired
//    Timer requestTimer;
//
//    @Autowired
//    MeterRegistry registry;

    AtomicInteger count = new AtomicInteger();

    private String requestSpeed = "line.request.speed";

    @Scheduled(fixedRate=1000)
    public void rateMetricConsole(){
        int andSet = count.getAndSet(0);

        System.out.println(andSet);
    }

    private Random random = new Random();


    public void generateAndSendEvent(RestTemplate template, HttpHeaders headers){

        count.getAndIncrement();

        DeviceEvent event = DeviceEvent.newBuilder()
                .setDeviceId(String.valueOf(random.nextInt(10)))
                .setComponentId(String.valueOf(random.nextInt(10)))
                .setEventId(String.valueOf(random.nextInt(10)))
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
