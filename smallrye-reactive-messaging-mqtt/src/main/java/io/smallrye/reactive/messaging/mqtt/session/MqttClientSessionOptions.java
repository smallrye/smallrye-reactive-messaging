package io.smallrye.reactive.messaging.mqtt.session;

import java.util.Optional;

import io.vertx.mqtt.MqttClientOptions;

public class MqttClientSessionOptions extends MqttClientOptions {

    private static final ReconnectDelayOptions DEFAULT_RECONNECT_DELAY = new ConstantReconnectDelayOptions();
    private static final Optional<String> DEFAULT_SERVER_NAME = Optional.empty();

    private String hostname = MqttClientOptions.DEFAULT_HOST;
    private Optional<String> serverName = DEFAULT_SERVER_NAME;
    private int port = MqttClientOptions.DEFAULT_PORT;
    private ReconnectDelayOptions reconnectDelay = DEFAULT_RECONNECT_DELAY;

    /**
     * Default constructor
     */
    public MqttClientSessionOptions() {
        super();
    }

    /**
     * Copy constructor
     *
     * @param other the options to copy
     */
    public MqttClientSessionOptions(MqttClientSessionOptions other) {
        super(other);
        this.hostname = other.hostname;
        this.port = other.port;
        this.serverName = other.serverName;
        this.reconnectDelay = other.reconnectDelay.copy();
    }

    public int getPort() {
        return this.port;
    }

    public MqttClientSessionOptions setPort(int port) {
        this.port = port;
        return this;
    }

    public String getHostname() {
        return this.hostname;
    }

    public MqttClientSessionOptions setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public MqttClientSessionOptions setReconnectDelay(ReconnectDelayOptions reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
        return this;
    }

    public ReconnectDelayOptions getReconnectDelay() {
        return this.reconnectDelay;
    }

    public Optional<String> getServerName() {
        return serverName;
    }

    public MqttClientSessionOptions setServerName(Optional<String> serverName) {
        this.serverName = serverName;
        return this;
    }
}
