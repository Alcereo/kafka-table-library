package com.github.alcereo.kafkatable.consumer;

import com.github.alcereo.kafkatable.producer.Application;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("lines")
public class LinesController {

    @Autowired
    private LinesService lines;

    @Autowired
    private KafkaStreams kafkaStreams;

    @GetMapping("old")
    public String getLines(){

        StringBuilder builder = new StringBuilder();

        builder.append("========== Lines =============").append("\n");

        lines.getLines().forEach((key, value) ->
                builder.append("## ")
                        .append(key).append(" - ")
                        .append(value).append("\n")
        );

        builder.append("=========== END ==============").append("\n");

        return builder.toString();

    }

    @GetMapping
    public String getTable(){

        ReadOnlyKeyValueStore<String, Long> keyValueStore = kafkaStreams.store(
                Application.TABLE_STORE,
                QueryableStoreTypes.keyValueStore()
        );

        StringBuilder builder = new StringBuilder();

        builder.append("=== Consumer store ===").append("\n");

        builder.append("========== Lines =============").append("\n");

        KeyValueIterator<String, Long> all = keyValueStore.all();

        while (all.hasNext()){
            KeyValue<String, Long> next = all.next();
            builder.append("## ").append(next.key).append(" - ").append(next.value).append("\n");
        }

        builder.append("=========== END ==============").append("\n");

        return builder.toString();
    }

}
