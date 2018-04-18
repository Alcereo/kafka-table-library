package com.github.alcereo.kafkatable.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping
@SpringBootApplication
public class AdminController {

    private static final String BROKERS = "192.170.0.3:9092";

    public static void main(String[] args) {
        SpringApplication.run(AdminController.class, args);
    }

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(){
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        return AdminClient.create(config);
    }

    @Autowired
    private AdminClient adminClient;


    @DeleteMapping("topics")
    public void deleteTopic(@RequestBody String name) throws ExecutionException, InterruptedException {

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(name));

        deleteTopicsResult.all().get();
    }


}
