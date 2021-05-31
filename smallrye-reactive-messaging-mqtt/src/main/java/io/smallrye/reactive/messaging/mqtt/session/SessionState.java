package io.smallrye.reactive.messaging.mqtt.session;

/**
 * The state of the session.
 */
public enum SessionState {
    /**
     * The session is disconnected.
     * <p>
     * A re-connect timer may be pending.
     */
    DISCONNECTED,
    /**
     * The session started to connect.
     * <p>
     * This may include re-subscribing to any topics after the connect call was successful.
     */
    CONNECTING,
    /**
     * The session is connected.
     */
    CONNECTED,
    /**
     * The session is in the process of an orderly disconnect.
     */
    DISCONNECTING,
}
