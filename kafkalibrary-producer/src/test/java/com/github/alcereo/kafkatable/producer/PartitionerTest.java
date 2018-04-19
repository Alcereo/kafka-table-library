package com.github.alcereo.kafkatable.producer;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.github.alcereo.kafkatable.producer.Application.PARTITIONER_NUMPARTS_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;

public class PartitionerTest {

    @Test
    public void partitionerTest() {

        Application.DeviceEventsPartitioner partitioner = new Application.DeviceEventsPartitioner();
        Map<String, Object> config = new HashMap<>();
        config.put(PARTITIONER_NUMPARTS_PROPERTY_NAME, 20);
        partitioner.configure(config);

        assertEquals(
                3,
                partitioner.partition(null, 3,null, null, null, null)
        );
        assertEquals(
                18,
                partitioner.partition(null, 18,null, null, null, null)
        );
        assertEquals(
                0,
                partitioner.partition(null, 20,null, null, null, null)
        );
        assertEquals(
                5,
                partitioner.partition(null, 25,null, null, null, null)
        );
    }

}
