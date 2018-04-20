package com.github.alcereo.kafkatool;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    static final String DEVICE_BUSINESS_STATUS_TABLE = "device-business-status-table";
    static final Integer NUM_PARTS = 20;
    static final String PARTITIONER_NUMPARTS_PROPERTY_NAME = "device.partitioner.numparts";

    static final String EVENT_TOPIC = "event-topic";

    static final String BROKERS = "192.170.0.3:9092";
    static final String SCHEMA_REGISTRY_URL = "http://192.170.0.6:8081";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
