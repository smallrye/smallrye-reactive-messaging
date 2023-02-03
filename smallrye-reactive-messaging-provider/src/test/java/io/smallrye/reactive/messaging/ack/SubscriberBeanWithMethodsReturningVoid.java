package io.smallrye.reactive.messaging.ack;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

/**
 * It is not supported to return `void` or an object (that it not a CompletionStage) when consuming a Message. So, we are
 * checking method returning
 * `CompletionStage<Void>`.
 */
@ApplicationScoped
public class SubscriberBeanWithMethodsReturningVoid extends SpiedBeanHelper {

    public static final String MANUAL_ACKNOWLEDGMENT = "manual-acknowledgment";
    public static final String NO_ACKNOWLEDGMENT = "no-acknowledgment";
    public static final String DEFAULT_ACKNOWLEDGMENT = "default-ack";
    public static final String PRE_PROCESSING_ACKNOWLEDGMENT = "pre-processing-ack";
    public static final String POST_PROCESSING_ACKNOWLEDGMENT = "post-processing-ack";

    @Incoming(MANUAL_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public CompletionStage<Void> subWithAck(Message<String> message) {
        processed(MANUAL_ACKNOWLEDGMENT, message.getPayload());
        // We cannot return a value or void by spec
        // So we return the CompletionStage<Void>.
        return message.ack();
    }

    @Outgoing(MANUAL_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToManualAck() {
        return source(MANUAL_ACKNOWLEDGMENT);
    }

    @Incoming(NO_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public void subWithNoAck(String payload) {
        processed(NO_ACKNOWLEDGMENT, payload);
    }

    @Outgoing(NO_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToNoAck() {
        return source(NO_ACKNOWLEDGMENT);
    }

    @Incoming(DEFAULT_ACKNOWLEDGMENT)
    public void subWithAutoAck(String payload) {
        processed(DEFAULT_ACKNOWLEDGMENT, payload);
    }

    @Outgoing(DEFAULT_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToAutoAck() {
        return source(DEFAULT_ACKNOWLEDGMENT);
    }

    @Incoming(PRE_PROCESSING_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.PRE_PROCESSING)
    public void subWithPreAck(String payload) {
        processed(PRE_PROCESSING_ACKNOWLEDGMENT, payload);
    }

    @Outgoing(PRE_PROCESSING_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToPreAck() {
        return source(PRE_PROCESSING_ACKNOWLEDGMENT);
    }

    @Incoming(POST_PROCESSING_ACKNOWLEDGMENT)
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public void subWithPostAck(String payload) {
        processed(POST_PROCESSING_ACKNOWLEDGMENT, payload);
    }

    @Outgoing(POST_PROCESSING_ACKNOWLEDGMENT)
    public Publisher<Message<String>> sourceToPostAck() {
        return source(POST_PROCESSING_ACKNOWLEDGMENT);
    }

}
