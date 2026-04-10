package io.smallrye.reactive.messaging.mqtt.session;

public interface SessionEvent {
    SessionState getSessionState();

    Throwable getCause();
}
