package io.smallrye.reactive.messaging.amqp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * An implementation of config source selecting a set of property based on the {@code mp-config} system property.
 */
public class VarConfigSource implements ConfigSource {

    private Map<String, String> INCOMING_BEAN_CONFIG;
    private Map<String, String> OUTGOING_BEAN_CONFIG;
    private Map<String, String> APP_GENERATING_DATA_CONFIG;
    private Map<String, String> APP_PROCESSING_DATA_CONFIG;

    private void init() {
        INCOMING_BEAN_CONFIG = new HashMap<>();
        OUTGOING_BEAN_CONFIG = new HashMap<>();
        APP_GENERATING_DATA_CONFIG = new HashMap<>();
        APP_PROCESSING_DATA_CONFIG = new HashMap<>();

        String prefix = "mp.messaging.incoming.data.";
        INCOMING_BEAN_CONFIG.put(prefix + "address", "data");
        INCOMING_BEAN_CONFIG.put(prefix + "connector", AmqpConnector.CONNECTOR_NAME);
        INCOMING_BEAN_CONFIG.put(prefix + "host", System.getProperty("amqp-host"));
        INCOMING_BEAN_CONFIG.put(prefix + "port", System.getProperty("amqp-port"));
        INCOMING_BEAN_CONFIG.put("amqp-username", System.getProperty("amqp-user"));
        INCOMING_BEAN_CONFIG.put("amqp-password", System.getProperty("amqp-pwd"));
        String optionsName = System.getProperty("client-options-name");
        if (optionsName != null) {
            INCOMING_BEAN_CONFIG.put(prefix + "client-options-name", optionsName);
        }

        prefix = "mp.messaging.outgoing.sink.";
        OUTGOING_BEAN_CONFIG.put(prefix + "address", "sink");
        OUTGOING_BEAN_CONFIG.put(prefix + "connector", AmqpConnector.CONNECTOR_NAME);
        OUTGOING_BEAN_CONFIG.put(prefix + "host", System.getProperty("amqp-host"));
        OUTGOING_BEAN_CONFIG.put(prefix + "port", System.getProperty("amqp-port"));
        OUTGOING_BEAN_CONFIG.put(prefix + "durable", "true");
        OUTGOING_BEAN_CONFIG.put("amqp-username", System.getProperty("amqp-user"));
        OUTGOING_BEAN_CONFIG.put("amqp-password", System.getProperty("amqp-pwd"));

        optionsName = System.getProperty("client-options-name");
        if (optionsName != null) {
            OUTGOING_BEAN_CONFIG.put(prefix + "client-options-name", optionsName);
        }

        prefix = "mp.messaging.outgoing.amqp.";
        APP_GENERATING_DATA_CONFIG.put(prefix + "connector", AmqpConnector.CONNECTOR_NAME);
        APP_GENERATING_DATA_CONFIG.put(prefix + "address", "should-not-be-used");
        APP_GENERATING_DATA_CONFIG.put(prefix + "durable", "true");
        APP_GENERATING_DATA_CONFIG.put(prefix + "host", System.getProperty("amqp-host"));
        APP_GENERATING_DATA_CONFIG.put(prefix + "port", System.getProperty("amqp-port"));
        APP_GENERATING_DATA_CONFIG.put("amqp-username", System.getProperty("amqp-user"));
        APP_GENERATING_DATA_CONFIG.put("amqp-password", System.getProperty("amqp-pwd"));

        prefix = "mp.messaging.outgoing.amqp.";
        APP_PROCESSING_DATA_CONFIG.put(prefix + "connector", AmqpConnector.CONNECTOR_NAME);
        APP_PROCESSING_DATA_CONFIG.put(prefix + "address", "my-address");
        APP_PROCESSING_DATA_CONFIG.put(prefix + "durable", "true");
        APP_PROCESSING_DATA_CONFIG.put(prefix + "host", System.getProperty("amqp-host"));
        APP_PROCESSING_DATA_CONFIG.put(prefix + "port", System.getProperty("amqp-port"));
        APP_PROCESSING_DATA_CONFIG.put("amqp-username", System.getProperty("amqp-user"));
        APP_PROCESSING_DATA_CONFIG.put("amqp-password", System.getProperty("amqp-pwd"));
        prefix = "mp.messaging.incoming.source.";
        APP_PROCESSING_DATA_CONFIG.put(prefix + "connector", AmqpConnector.CONNECTOR_NAME);
        APP_PROCESSING_DATA_CONFIG.put(prefix + "address", "my-source");
        APP_PROCESSING_DATA_CONFIG.put(prefix + "host", System.getProperty("amqp-host"));
        APP_PROCESSING_DATA_CONFIG.put(prefix + "port", System.getProperty("amqp-port"));
    }

    @Override
    public Map<String, String> getProperties() {
        init();
        String property = System.getProperty("mp-config");
        if ("incoming".equalsIgnoreCase(property)) {
            return INCOMING_BEAN_CONFIG;
        }
        if ("outgoing".equalsIgnoreCase(property)) {
            return OUTGOING_BEAN_CONFIG;
        }
        if ("app-generating-data".equalsIgnoreCase(property)) {
            return APP_GENERATING_DATA_CONFIG;
        }
        if ("app-processing-data".equalsIgnoreCase(property)) {
            return APP_PROCESSING_DATA_CONFIG;
        }
        return Collections.emptyMap();
    }

    @Override
    public String getValue(String propertyName) {
        return getProperties().get(propertyName);
    }

    @Override
    public String getName() {
        return "var-config-source";
    }
}
