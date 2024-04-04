package io.smallrye.reactive.messaging.providers.helpers;

import io.smallrye.reactive.messaging.json.JsonMapping;
import io.vertx.core.json.Json;

public class VertxJsonMapping implements JsonMapping {

    @Override
    public String toJson(Object object) {
        return Json.encode(object);
    }

    @Override
    public <T> T fromJson(String str, Class<T> type) {
        return Json.decodeValue(str, type);
    }
}
