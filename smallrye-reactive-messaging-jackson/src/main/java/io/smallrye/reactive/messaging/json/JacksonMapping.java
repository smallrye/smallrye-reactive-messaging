package io.smallrye.reactive.messaging.json;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@ApplicationScoped
@Priority(value = JsonMapping.DEFAULT_PRIORITY)
public class JacksonMapping implements JsonMapping {

    @Inject
    ObjectMapper objectMapper;

    @Override
    public String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T fromJson(String str, Class<T> type) {
        try {
            return objectMapper.readValue(str, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
