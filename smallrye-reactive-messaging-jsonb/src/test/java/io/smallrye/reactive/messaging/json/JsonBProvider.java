package io.smallrye.reactive.messaging.json;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;

/**
 * Provider of {@link Jsonb}, should be done by the "app-server" (Quarkus does provide an injectable).
 */
@ApplicationScoped
public class JsonBProvider {
    @Produces
    public Jsonb jsonb() {
        return JsonbBuilder.create();
    }
}
