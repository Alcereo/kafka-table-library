package com.github.alcereo.kafkatable.loadgen;


import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@EnableScheduling
@SpringBootApplication
public class LinesScheduler {




    public static void main(String[] args) {
        SpringApplication.run(LinesScheduler.class, args);
    }

    @Bean
    public RestTemplate template() {
        return new RestTemplateBuilder ()
                .rootUri("http://localhost:8080/")
                .build();
    }


    private List<String> lines = new ArrayList<>();

    public LinesScheduler() {

        lines.addAll(
                Arrays.asList(
                        "one",
                        "second",
                        "third",
                        "another",
                        "and-another"
                )
        );

        ExecutorService exec = Executors.newCachedThreadPool();

        for (int i = 0; i < 30; i++) {
            exec.execute(() -> {
                RestTemplate build = new RestTemplateBuilder()
                        .rootUri("http://localhost:8080/")
                        .build();

                while (!Thread.currentThread().isInterrupted()) {
                    RequestLine(build);
                }
            });
        }
    }

    @Autowired
    RestTemplate template;


    @Bean
    public Timer requestTimer(MeterRegistry registry){
        return Timer
                .builder("line.request.timer")
                .register(registry);
    }


    @Bean
    public Gauge requestSpeedCounter(MeterRegistry registry){
        return Gauge.builder(requestSpeed, 0d, value -> value)
                .register(registry);
    }

    @Autowired
    Gauge requestSpeedCounter;

    @Autowired
    Timer requestTimer;

    @Autowired
    MeterRegistry registry;

    AtomicInteger count = new AtomicInteger();

    private String requestSpeed = "line.request.speed";

    @Scheduled(fixedRate=1000)
    public void RequestLineSpeed(){
        int andSet = count.getAndSet(0);

        System.out.println(andSet);
    }

    private Random random = new Random();

    public void RequestLine(RestTemplate template){

        count.getAndIncrement();

        int randomIndex = Math.abs(random.nextInt() % lines.size());

        template.postForEntity(
                "http://localhost:8080/lines",
                lines.get(randomIndex),
                String.class
        );

    }

}
