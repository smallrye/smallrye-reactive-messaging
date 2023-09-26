package io.smallrye.reactive.messaging;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Container of {@link Message}s to send multiple messages to different channels
 * <p>
 * The {@link Default} implementation holds channel-message mappings in an unmodifiable {@link Map}.
 * This implementation returns the inner map from the {@link Message#getPayload()}
 */
public interface TargetedMessages extends Message<Map<String, Message<?>>> {

    /**
     *
     * @param payloads
     * @return
     */
    static TargetedMessages from(Targeted payloads) {
        Map<String, Message<?>> map = new HashMap<>();
        for (Map.Entry<String, Object> entry : payloads) {
            map.put(entry.getKey(), Message.of(entry.getValue()));
        }
        return from(map);
    }

    static TargetedMessages from(Map<String, Message<?>> entries) {
        return new Default(entries);
    }

    static TargetedMessages of(String channel, Message<?> message) {
        return from(Map.of(channel, message));
    }

    static TargetedMessages of(String c1, Message<?> m1, String c2, Message<?> m2) {
        return from(Map.of(c1, m1, c2, m2));
    }

    static TargetedMessages of(String c1, Message<?> m1, String c2, Message<?> m2, String c3, Message<?> m3) {
        return from(Map.of(c1, m1, c2, m2, c3, m3));
    }

    static TargetedMessages of(String c1, Message<?> m1, String c2, Message<?> m2, String c3, Message<?> m3,
            String c4, Message<?> m4) {
        return from(Map.of(c1, m1, c2, m2, c3, m3, c4, m4));
    }

    static TargetedMessages of(String c1, Message<?> m1, String c2, Message<?> m2, String c3, Message<?> m3,
            String c4, Message<?> m4, String c5, Message<?> m5) {
        return from(Map.of(c1, m1, c2, m2, c3, m3, c4, m4, c5, m5));
    }

    static TargetedMessages of(String c1, Message<?> m1, String c2, Message<?> m2, String c3, Message<?> m3,
            String c4, Message<?> m4, String c5, Message<?> m5, String c6, Message<?> m6) {
        return from(Map.of(c1, m1, c2, m2, c3, m3, c4, m4, c5, m5, c6, m6));
    }

    /**
     * Return the message associated with the given channel
     *
     * @param channel the channel name
     * @return the {@link Message} associated with the channel. Returns {@code null} if this mapping does not contain the given
     *         channel.
     */
    Message<?> get(String channel);

    /**
     * Add a new mapping to this target mapping.
     * It returns a new {@link TargetedMessages} instance.
     *
     * @param channel the channel to add
     * @param message the message to add
     * @return a new instance of {@link TargetedMessages}
     */
    TargetedMessages withMessage(String channel, Message<?> message);

    /**
     * Add a new mapping to this target mapping.
     * If the given object is not an instance of {@link Message}, it'll be wrapped as a message
     *
     * @param channel the channel to add
     * @param payloadOrMessage the payload or message to add
     * @return a new instance of {@link Targeted}
     */
    TargetedMessages with(String channel, Object payloadOrMessage);

    /**
     * Default {@link TargetedMessages} implementation
     */
    class Default implements TargetedMessages {

        private final Map<String, Message<?>> backend;

        public Default(Map<String, Message<?>> map) {
            this.backend = Map.copyOf(map);
        }

        @Override
        public Message<?> get(String channel) {
            return backend.get(channel);
        }

        @Override
        public TargetedMessages withMessage(String channel, Message<?> message) {
            Map<String, Message<?>> map = new HashMap<>(backend);
            map.put(channel, message);
            return new Default(map);
        }

        @Override
        public TargetedMessages with(String channel, Object payload) {
            if (payload instanceof Message) {
                return withMessage(channel, (Message<?>) payload);
            }
            return withMessage(channel, Message.of(payload));
        }

        @Override
        public Map<String, Message<?>> getPayload() {
            return backend;
        }

    }

}
