package com.github.alcereo.kafkatable.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import processing.DeviceBusinessStatus;
import processing.DeviceEvent;

import java.util.Arrays;
import java.util.UUID;

import static com.github.alcereo.kafkatable.consumer.Application.DEVICE_BUSINESS_STATUS_TABLE;

@Component
@Slf4j
public class EventProcessing {

    @Autowired
    KafkaConsumer<Integer, DeviceEvent> eventsConsumer;

    @Autowired
    KafkaProducer<Integer, DeviceBusinessStatus> producer;


    @Autowired
    KafkaConsumer<Integer, DeviceBusinessStatus> deviceStateConsumer;

    @Autowired
    EventInMemoryStore store;

    @Autowired
    DeviceBusinessStateInMemoryStore deviceStatusesStore;

//    TODO: reimplement when will go to multitreding
    private static final byte[] sessionUUID = UUID.randomUUID().toString().getBytes();

    @Scheduled(fixedRate = 1)
    public void pollMessages(){

        ConsumerRecords<Integer, DeviceEvent> records = eventsConsumer.poll(300);

        records.forEach(
                record -> {
                    DeviceEvent deviceEvent = record.value();
                    store.addEvent(deviceEvent);

                    if (deviceEvent.getEventId().equals("1")) {

                        DeviceBusinessStatus errorStatus = DeviceBusinessStatus.newBuilder()
                                .setStatus("ERROR")
                                .build();

                        ProducerRecord<Integer, DeviceBusinessStatus> errorRecord = new ProducerRecord<>(
                                DEVICE_BUSINESS_STATUS_TABLE,
                                deviceEvent.getDeviceId(),
                                errorStatus
                        );

                        errorRecord.headers().add("session", sessionUUID);
                        producer.send(errorRecord);
                        producer.flush();

                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), errorRecord.value());
                    } else if (deviceEvent.getEventId().equals("2")){
                        ProducerRecord<Integer, DeviceBusinessStatus> fineRecord = new ProducerRecord<>(
                                DEVICE_BUSINESS_STATUS_TABLE,
                                deviceEvent.getDeviceId(),
                                DeviceBusinessStatus.newBuilder()
                                        .setStatus("FINE")
                                        .build()
                        );

                        fineRecord.headers().add("session", sessionUUID);
                        producer.send(fineRecord);
                        producer.flush();

                        deviceStatusesStore.upsert(deviceEvent.getDeviceId(), fineRecord.value());
                    }
                }
        );

        eventsConsumer.commitSync();

    }


    @Scheduled(fixedRate = 1)
    public void pollBusinessStatuses(){

        ConsumerRecords<Integer, DeviceBusinessStatus> records = deviceStateConsumer.poll(300);

        records.forEach(record -> {
            log.trace("Don't save record by header");

            if (record.headers().lastHeader("session") != null){
                if (!Arrays.equals(record.headers().lastHeader("session").value(), sessionUUID)){
                    deviceStatusesStore.externalUpsert(record.key(), record.value());
                }else {
                    log.trace("Don't save record by header");
                }
            } else {
                deviceStatusesStore.externalUpsert(record.key(), record.value());
            }
        });

    }

}
