package io.smallrye.reactive.messaging.cloudevents;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;

/**
 * Message extending Cloud Events.
 * 
 * @param <T> the type of payload.
 */
public interface CloudEventMessage<T> extends CloudEvent<AttributesImpl, T>, Message<T> {

}
