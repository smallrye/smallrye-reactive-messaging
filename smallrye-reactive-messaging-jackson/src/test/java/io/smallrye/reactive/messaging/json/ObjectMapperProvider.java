package io.smallrye.reactive.messaging.json;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Provider of {@link ObjectMapper}, should be done by the "app-server" (Quarkus does provide an injectable).
 */
@ApplicationScoped
public class ObjectMapperProvider {
    @Produces
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
