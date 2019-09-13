package io.smallrye.reactive.messaging.aws.sns;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.amazonaws.services.sns.message.SnsNotification;

/**
 * Implementation of ractive messaging org.eclipse.microprofile.reactive.messaging.Message<T>
 * 
 * @author iabughosh
 * @version 1.0.4
 *
 * @param <T> currenlty, it only supports java.lang.String.
 */
public class SnsMessage<T> implements Message<T> {

    /** Instance of SNS notification message */
    private final SnsNotification snsMessage;
    /** String payload, used for testing */
    private final T payload;

    /**
     * Constructor for AWS SNS.
     * 
     * @param snsMessage
     */
    public SnsMessage(SnsNotification snsMessage) {
        Objects.requireNonNull(snsMessage, "SNS Message cannot be null.");
        this.snsMessage = snsMessage;
        payload = null;
    }

    /**
     * Constructor for fake SNS.
     * 
     * @param payload
     */
    public SnsMessage(T payload) {
        this.payload = payload;
        snsMessage = null;
    }

    @Override
    public CompletionStage<Void> ack() {
        //Acknowledgment is handled automatically by AWS SDK.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public T getPayload() {
        return snsMessage != null ? (T) snsMessage.getMessage() : payload;
    }

    /**
     * Get message subject of SNS notification.
     * 
     * @return message subject.
     */
    public String getSubject() {
        return snsMessage.getSubject();
    }
}
