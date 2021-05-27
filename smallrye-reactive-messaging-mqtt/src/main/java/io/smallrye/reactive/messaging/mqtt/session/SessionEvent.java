package io.smallrye.reactive.messaging.mqtt.session;

import io.vertx.codegen.annotations.VertxGen;

@VertxGen
public interface SessionEvent {
    SessionState getSessionState();

    Throwable getCause();
}
