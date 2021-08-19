package io.smallrye.reactive.messaging.json;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

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
