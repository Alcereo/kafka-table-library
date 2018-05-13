package com.github.alcereo.kafkatool.sample.producer;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;
import processing.DeviceEvent;

import java.time.Instant;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "kafka.brokers=${spring.embedded.kafka.brokers}",
                "management.metrics.export.influx.enabled=false"
        })
public class EventsControllerTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1,
            true,
            "event-topic");

    @Test
    public void greetingShouldReturnDefaultMessage() throws Exception {

        DeviceEvent event = DeviceEvent.newBuilder()
                .setComponentId("1")
                .setDeviceId(1)
                .setEventId("1")
                .setTimestamp(Instant.now().toString())
                .build();

        HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(event.toString(), requestHeaders);

        assertThat(
                restTemplate.postForObject(
                        "http://localhost:" + port + "/event",
                        request,
                        String.class
                )
        ).contains("success");

//        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
//        embeddedKafka.consumeFromAnEmbeddedTopic(
//                consumer,
//                "event-topic"
//        );
//
//        ConsumerRecords<Object, Object> poll = consumer.poll(100);
//
//        System.out.println(poll);

    }

}