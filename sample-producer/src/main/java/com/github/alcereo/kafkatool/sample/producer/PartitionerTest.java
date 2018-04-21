package com.github.alcereo.kafkatool.sample.producer;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.github.alcereo.kafkatool.sample.producer.Application.PARTITIONER_NUMPARTS_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;

public class PartitionerTest {

    @Test
    public void partitionerTest() {

        Application.DeviceEventsPartitioner partitioner = new Application.DeviceEventsPartitioner();
        Map<String, Object> config = new HashMap<>();
        config.put(PARTITIONER_NUMPARTS_PROPERTY_NAME, 20);
        partitioner.configure(config);

        Assert.assertEquals(
                3,
                partitioner.partition(null, 3,null, null, null, null)
        );
        Assert.assertEquals(
                18,
                partitioner.partition(null, 18,null, null, null, null)
        );
        Assert.assertEquals(
                0,
                partitioner.partition(null, 20,null, null, null, null)
        );
        Assert.assertEquals(
                5,
                partitioner.partition(null, 25,null, null, null, null)
        );
    }

}
