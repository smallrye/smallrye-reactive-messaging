package io.smallrye.reactive.messaging.mqtt.session;

public interface SessionEvent {
    SessionState getSessionState();

    Throwable getCause();

    /**
     * @return the CONNACK reason code (MQTT 5.0), or {@code null} if not available
     */
    default Integer getReasonCode() {
        return null;
    }
}
