package io.smallrye.reactive.messaging.memory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.memory.i18n.InMemoryExceptions;

/**
 * An implementation of connector used for testing applications without having to use external broker.
 * The idea is to substitute the {@code connector} of a specific channel to use {@code smallrye-in-memory}.
 * Then, your test can send message and checked the received messages.
 *
 * @deprecated Use {@link TestingConnector} with connector name {@code smallrye-testing} instead.
 */
@Deprecated(forRemoval = true)
@ApplicationScoped
@Connector(InMemoryConnector.CONNECTOR)
public class InMemoryConnector extends TestingConnector {

    public static final String CONNECTOR = "smallrye-in-memory";

    /**
     * Switch the given <em>incoming</em> channel to in-memory. It replaces the previously used connector with the
     * in-memory connector.
     *
     * @param channels the channels to switch, must not be {@code null}, must not contain {@code null}, must not contain
     *        a blank value
     * @return The map of properties that have been defined. The method sets the system properties, but give
     *         you this map to pass the properties around if needed.
     * @deprecated Use {@link TestingConnector#switchIncomingChannelsToTesting(String...)} instead.
     */
    @Deprecated(forRemoval = true)
    public static Map<String, String> switchIncomingChannelsToInMemory(String... channels) {
        Map<String, String> properties = new LinkedHashMap<>();
        for (String channel : channels) {
            if (channel == null || channel.trim().isEmpty()) {
                throw InMemoryExceptions.ex.illegalArgumentChannelNameNull();
            }
            String key = "mp.messaging.incoming." + channel + ".connector";
            properties.put(key, CONNECTOR);
            System.setProperty(key, CONNECTOR);
        }
        return properties;
    }

    /**
     * Switch the given <em>outgoing</em> channel to in-memory. It replaces the previously used connector with the
     * in-memory connector.
     *
     * @param channels the channels to switch, must not be {@code null}, must not contain {@code null}, must not contain
     *        a blank value
     * @return The map of properties that have been defined. The method sets the system properties, but give
     *         you this map to pass these properties around if needed.
     * @deprecated Use {@link TestingConnector#switchOutgoingChannelsToTesting(String...)} instead.
     */
    @Deprecated(forRemoval = true)
    public static Map<String, String> switchOutgoingChannelsToInMemory(String... channels) {
        Map<String, String> properties = new LinkedHashMap<>();
        for (String channel : channels) {
            if (channel == null || channel.trim().isEmpty()) {
                throw InMemoryExceptions.ex.illegalArgumentChannelNameNull();
            }
            String key = "mp.messaging.outgoing." + channel + ".connector";
            properties.put(key, CONNECTOR);
            System.setProperty(key, CONNECTOR);
        }
        return properties;
    }

    /**
     * Switch back the channel to their original connector.
     *
     * @deprecated Use {@link TestingConnector#clear()} instead.
     */
    @Deprecated(forRemoval = true)
    public static void clear() {
        List<String> list = System.getProperties().entrySet().stream()
                .filter(entry -> CONNECTOR.equals(entry.getValue())
                        || TestingConnector.CONNECTOR.equals(entry.getValue()))
                .map(entry -> (String) entry.getKey())
                .collect(Collectors.toList());
        list.forEach(System::clearProperty);
    }

    /**
     * @deprecated Use {@link TestingConnector#incoming(String)} instead.
     */
    @Override
    @Deprecated(forRemoval = true)
    @SuppressWarnings("unchecked")
    public <T> InMemorySource<T> source(String channel) {
        return (InMemorySource<T>) super.incoming(channel);
    }

    /**
     * @deprecated Use {@link TestingConnector#outgoing(String)} instead.
     */
    @Override
    @Deprecated(forRemoval = true)
    @SuppressWarnings("unchecked")
    public <T> InMemorySink<T> sink(String channel) {
        return (InMemorySink<T>) super.outgoing(channel);
    }
}
