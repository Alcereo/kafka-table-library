package com.github.alcereo.kafkatool.sample.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.*;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class Application implements CommandLineRunner {

    static final String DEVICE_BUSINESS_STATUS_TABLE = "device-business-status-table";
    static final Integer NUM_PARTS = 20;

    static final String EVENT_TOPIC = "event-topic";


    static final String BROKERS = "192.170.0.3:9092";
    static final String SCHEMA_REGISTRY_URL = "http://192.170.0.6:8081";

    public static void main(String[] args){
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws ExecutionException, InterruptedException {

        log.info("Intent to create topic: "+DEVICE_BUSINESS_STATUS_TABLE);

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        try(AdminClient adminClient = AdminClient.create(config)) {

            Set<String> strings = adminClient.listTopics().names().get();

            if (!strings.contains(DEVICE_BUSINESS_STATUS_TABLE)) {

//        BUSINESS_STATUS_TOPIC ++
                NewTopic businessStatusTopic = new NewTopic(DEVICE_BUSINESS_STATUS_TABLE, NUM_PARTS, (short) 1);
                Map<String, String> properties = new HashMap<>();
                properties.put("cleanup.policy", "compact");
//                properties.put("delete.retention.ms", "20000");
//                properties.put("segment.ms", "20000");
//                properties.put("min.cleanable.dirty.ratio", "0.01");

                businessStatusTopic.configs(properties);

//        BUSINESS_STATUS_TOPIC --

                CreateTopicsResult result = adminClient.createTopics(
                        Collections.singletonList(businessStatusTopic)
                );

                result.all().get();
            }
        }

    }

}
