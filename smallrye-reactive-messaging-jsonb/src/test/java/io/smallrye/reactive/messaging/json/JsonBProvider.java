package io.smallrye.reactive.messaging.json;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

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
