package com.github.alcereo.kafkatool;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.Optional;

@Builder
@Getter
public class KtContext {

    MeterRegistry meterRegistry;

    @NonNull
    private String brokers;

    private String schemaRegistryUrl;



    public Optional<MeterRegistry> getMeterRegistry() {
        return Optional.ofNullable(meterRegistry);
    }

    public Optional<String> schemaRegistryUrl(){
        return Optional.ofNullable(schemaRegistryUrl);
    }

}
