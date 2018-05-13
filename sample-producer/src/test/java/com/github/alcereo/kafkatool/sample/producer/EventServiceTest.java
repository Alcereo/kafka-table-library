package com.github.alcereo.kafkatool.sample.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import processing.DeviceEvent;

import static org.junit.Assert.assertEquals;

public class EventServiceTest {

    private EventService eventService;
    private MockProducer<Integer, DeviceEvent> testProducer;

    @Before
    public void setUp() {
        testProducer = new MockProducer<>();
        ProducerFactory<Integer, DeviceEvent> producerFactory = () -> testProducer;
        KafkaTemplate<Integer, DeviceEvent> producerTemplate = new SpringKafkaConfig().eventProducer(producerFactory);
        producerTemplate.setDefaultTopic("default-topic");
        eventService = new EventService(producerTemplate);
    }

    @Test
    public void testSendDevice() {

        DeviceEvent device = DeviceEvent.newBuilder()
                .setTimestamp("1")
                .setEventId("2")
                .setDeviceId(3)
                .setComponentId("4")
                .build();

        eventService.sendDeviceEvent(device);

        assertEquals(
                device.getDeviceId(),
                testProducer.history().get(0).key()
        );

        assertEquals(
                device,
                testProducer.history().get(0).value()
        );

    }
}