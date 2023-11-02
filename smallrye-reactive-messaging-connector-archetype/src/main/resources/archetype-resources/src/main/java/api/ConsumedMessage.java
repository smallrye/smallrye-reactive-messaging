package ${package}.api;

import java.util.Map;

/**
 * Message object received from the underlying library
 *
 * @param <T> payload type
 */
public interface ConsumedMessage<T> {

    String key();

    long timestamp();

    T body();

    Map<String, String> properties();

    String topic();

    String clientId();
}
