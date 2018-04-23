package com.github.alcereo.kafkatool.sample.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.config.EnableWebFlux;

import java.util.*;

@SpringBootApplication
@EnableScheduling
@EnableWebFlux
@Slf4j
public class Application implements CommandLineRunner {

    static final String EVENT_TOPIC = "event-topic";
    static final Integer NUM_PARTS = 20;
    static final String PARTITIONER_NUMPARTS_PROPERTY_NAME = "device.partitioner.numparts";

    static final String BROKERS = "192.170.0.3:9092";
    static final String SCHEMA_REGISTRY_URL = "http://192.170.0.6:8081";

//    static final String BROKERS = "35.227.92.147:9092,35.227.115.219:9092,35.227.112.251:9092";
//    static final String SCHEMA_REGISTRY_URL = "http://35.196.173.108:8081";

    public static void main(String[] args){
        new SpringApplicationBuilder()
                .web(WebApplicationType.REACTIVE)
                .sources(Application.class)
                .run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Intent to create topic: " + EVENT_TOPIC);

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        try(AdminClient adminClient = AdminClient.create(config)) {

            Set<String> strings = adminClient.listTopics().names().get();

            if (!strings.contains(EVENT_TOPIC)) {

//        EVENT_TOPIC ++
                NewTopic topic = new NewTopic(EVENT_TOPIC, NUM_PARTS, (short) 1);
                Map<String, String> properties = new HashMap<>();

                topic.configs(properties);

//        EVENT_TOPIC --

                CreateTopicsResult result = adminClient.createTopics(
                        Collections.singletonList(topic)
                );

                result.all().get();
            }
        }
    }
}
