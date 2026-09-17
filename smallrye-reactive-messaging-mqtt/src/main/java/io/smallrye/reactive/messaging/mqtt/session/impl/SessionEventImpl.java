package io.smallrye.reactive.messaging.mqtt.session.impl;

import io.smallrye.reactive.messaging.mqtt.session.SessionEvent;
import io.smallrye.reactive.messaging.mqtt.session.SessionState;

/**
 * An event of a session state change.
 */
public class SessionEventImpl implements SessionEvent {

    private final SessionState sessionState;
    private final Throwable cause;
    private final Integer reasonCode;

    public SessionEventImpl(final SessionState sessionState, final Throwable reason) {
        this(sessionState, reason, null);
    }

    public SessionEventImpl(final SessionState sessionState, final Throwable reason, final Integer reasonCode) {
        this.sessionState = sessionState;
        this.cause = reason;
        this.reasonCode = reasonCode;
    }

    /**
     * The new state of the session.
     *
     * @return The state.
     */
    @Override
    public SessionState getSessionState() {
        return this.sessionState;
    }

    /**
     * The (optional) cause of change.
     *
     * @return The throwable that causes the state change, or {@code null}, if there was none.
     */
    @Override
    public Throwable getCause() {
        return this.cause;
    }

    /**
     * The reason code from the CONNACK (MQTT 5.0).
     *
     * @return The reason code, or {@code null} if not available.
     */
    @Override
    public Integer getReasonCode() {
        return this.reasonCode;
    }
}
