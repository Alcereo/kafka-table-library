package com.github.alcereo.kafkatool.sample.producer;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.config.EnableWebFlux;

@SpringBootApplication
@EnableScheduling
@EnableWebFlux
@Slf4j
public class Application{

//    static final String BROKERS = "35.227.92.147:9092,35.227.115.219:9092,35.227.112.251:9092";
//    static final String SCHEMA_REGISTRY_URL = "http://35.196.173.108:8081";

    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder()
                .web(WebApplicationType.REACTIVE)
                .sources(Application.class)
                .run(args);
    }

    @Bean
    MeterRegistryCustomizer<MeterRegistry> metricsCommonTags(
            @Value("${spring.application.name}") String appName) {
        return registry -> registry.config().commonTags("app-name", appName);
    }
}
