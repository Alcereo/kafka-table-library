package com.github.alcereo.kafkatool.sample.producer;

import com.github.alcereo.kafkatool.producer.KtProducer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import processing.DeviceEvent;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RestController
@RequestMapping("loader")
public class LoadController {

    private ExecutorService exec = Executors.newCachedThreadPool();
    private volatile boolean running = true;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LoadData{
        Integer numThread;
        Integer idBound;
    }

    @PostMapping("start")
    public String startLoad(@RequestBody LoadData loadData){

        log.info("Start load with props: {}", loadData);
        running=true;

        startLoadgen(loadData.numThread, loadData.idBound);

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

    public void startLoadgen(Integer numThreads, Integer idBound) {

        log.info("Starting loadgen workers. Count: {}", numThreads);

        for (int i = 0; i < numThreads; i++) {

            exec.execute(() -> {

                while (!Thread.currentThread().isInterrupted() && running) {
                    generateAndSendEvent(idBound);
                }
            });
        }
    }

    public void generateAndSendEvent(Integer idBound){
        requestTimer.record(() -> {
            DeviceEvent event = DeviceEvent.newBuilder()
                    .setDeviceId(random.nextInt(idBound))
                    .setComponentId(String.valueOf(random.nextInt(3)))
                    .setEventId(String.valueOf(random.nextInt(6)))
                    .setTimestamp(String.valueOf(System.currentTimeMillis()))
                    .build();

            sendEvent(event);
        });
    }




    @Autowired
    private KtProducer<Integer, DeviceEvent> producer;

    private void sendEvent(DeviceEvent event) {
        try {
            producer.sendSync(event.getDeviceId(), event);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while sending message", e);
            throw new RuntimeException(e);
        }
    }

}
