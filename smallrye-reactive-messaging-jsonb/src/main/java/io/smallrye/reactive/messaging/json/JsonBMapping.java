package io.smallrye.reactive.messaging.json;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.Jsonb;

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
