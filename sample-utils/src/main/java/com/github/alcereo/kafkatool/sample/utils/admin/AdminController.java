package com.github.alcereo.kafkatool.sample.utils.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("topics")
public class AdminController {

    @Value("${kafka.brokers}")
    private String BROKERS = "192.170.0.3:9092";

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(){
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);

        return AdminClient.create(config);
    }

    @Autowired
    private AdminClient adminClient;


    @DeleteMapping
    public void deleteTopic(@RequestBody String name) throws ExecutionException, InterruptedException {

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(name));

        deleteTopicsResult.all().get();
    }


}
