package io.smallrye.reactive.messaging.cloudevents;

import io.cloudevents.CloudEvent;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Message extending Cloud Events.
 * @param <T> the type of payload.
 */
public interface CloudEventMessage<T> extends CloudEvent<T>, Message<T> {

}
