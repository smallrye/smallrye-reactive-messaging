package io.smallrye.reactive.messaging;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Container of payloads to send multiple messages to different channels
 * <p>
 * The {@link TargetedMessages.Default} implementation holds channel-payload mappings in an unmodifiable {@link Map}.
 */
public interface Targeted extends Iterable<Map.Entry<String, Object>> {

    static Targeted from(Map<String, Object> entries) {
        return new Default(entries);
    }

    static Targeted of(String channel, Object payload) {
        return from(Map.of(channel, payload));
    }

    static Targeted of(String c1, Object p1, String c2, Object p2) {
        return from(Map.of(c1, p1, c2, p2));
    }

    static Targeted of(String c1, Object p1, String c2, Object p2, String c3, Object p3) {
        return from(Map.of(c1, p1, c2, p2, c3, p3));
    }

    static Targeted of(String c1, Object p1, String c2, Object p2, String c3, Object p3, String c4, Object p4) {
        return from(Map.of(c1, p1, c2, p2, c3, p3, c4, p4));
    }

    static Targeted of(String c1, Object p1, String c2, Object p2, String c3, Object p3, String c4, Object p4,
            String c5, Object p5) {
        return from(Map.of(c1, p1, c2, p2, c3, p3, c4, p4, c5, p5));
    }

    static Targeted of(String c1, Object p1, String c2, Object p2, String c3, Object p3, String c4, Object p4,
            String c5, Object p5, String c6, Object p6) {
        return from(Map.of(c1, p1, c2, p2, c3, p3, c4, p4, c5, p5, c6, p6));
    }

    /**
     * Return the payload associated with the given channel
     *
     * @param channel the channel name
     * @return the payload associated with the channel. Returns {@code null} if this mapping does not contain the given channel.
     */
    Object get(String channel);

    /**
     * Add the given channel-payload mapping to this target mapping by returning a new one.
     * It returns a new {@link Targeted} instance.
     *
     * @param channel the channel to add
     * @param payload the payload to add
     * @return a new instance of {@link Targeted}
     */
    Targeted with(String channel, Object payload);

    /**
     * Default {@link Targeted} implementation
     */
    class Default implements Targeted {

        private final Map<String, Object> backend;

        public Default(Map<String, Object> map) {
            this.backend = Map.copyOf(map);
        }

        @Override
        public Object get(String channel) {
            return backend.get(channel);
        }

        @Override
        public Targeted with(String channel, Object payload) {
            Map<String, Object> copy = new HashMap<>(backend);
            copy.put(channel, payload);
            return new Default(copy);
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return backend.entrySet().iterator();
        }
    }

}
