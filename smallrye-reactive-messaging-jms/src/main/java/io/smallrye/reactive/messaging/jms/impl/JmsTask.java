package io.smallrye.reactive.messaging.jms.impl;

import java.util.function.Consumer;

import jakarta.jms.Message;

public interface JmsTask {
    void apply(Message message) throws Exception;

    static Consumer<Message> wrap(JmsTask task) {
        return msg -> {
            try {
                task.apply(msg);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
    }
}
