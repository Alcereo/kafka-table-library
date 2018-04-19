package com.github.alcereo.kafkatable.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;
import processing.DeviceEvent;

import static com.github.alcereo.kafkatable.consumer.Application.DEVICE_BUSINESS_STATUS;

@Component
public class EventProcessing {

    @Autowired
    KafkaConsumer<String, DeviceEvent> consumer;

    @Autowired
    KafkaProducer<Integer, DeviceBusinessStatus> producer;

    @Autowired
    EventInMemoryStore store;

    @Scheduled(fixedRate = 100)
    public void pollMessages(){

        ConsumerRecords<String, DeviceEvent> records = consumer.poll(300);

        records.forEach(
                record -> {
                    DeviceEvent deviceEvent = record.value();
                    store.addEvent(deviceEvent);

                    if (deviceEvent.getEventId().equals("1")) {
                        producer.send(
                                new ProducerRecord<>(
                                        DEVICE_BUSINESS_STATUS,
                                        deviceEvent.getDeviceId(),
                                        DeviceBusinessStatus.newBuilder()
                                                .setStatus("ERROR")
                                                .build()
                                )
                        );
                        producer.flush();
                    } else if (deviceEvent.getEventId().equals("2")){
                        producer.send(
                                new ProducerRecord<>(
                                        DEVICE_BUSINESS_STATUS,
                                        deviceEvent.getDeviceId(),
                                        DeviceBusinessStatus.newBuilder()
                                                .setStatus("FINE")
                                                .build()
                                )
                        );
                        producer.flush();
                    }
                }
        );

        consumer.commitSync();

    }



}
