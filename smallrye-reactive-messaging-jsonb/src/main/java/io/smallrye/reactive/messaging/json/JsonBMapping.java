package io.smallrye.reactive.messaging.json;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;

@ApplicationScoped
@Priority(value = JsonMapping.DEFAULT_PRIORITY + 1)
public class JsonBMapping implements JsonMapping {
    @Inject
    Jsonb jsonb;

    @Override
    public String toJson(Object object) {
        return jsonb.toJson(object);
    }

    @Override
    public <T> T fromJson(String str, Class<T> type) {
        return jsonb.fromJson(str, type);
    }
}
