package com.github.alcereo.kafkatable.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class LinesService {

    private Map<String, Long> lines = new HashMap<>();

    public void upsertLine(String line, Optional<Long> number){

        log.debug("=============>>> "+line+" -|||- "+number);

        if (number.isPresent()){
            lines.put(line, number.get());
        }else {
            lines.remove(line);
        }
    }

    public Map<String, Long> getLines(){
        return new HashMap<>(lines);
    }

}
