package io.smallrye.reactive.messaging.aws.sns;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.amazonaws.services.sns.message.SnsNotification;

public class SnsMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    private SnsNotification snsMessage;

    public SnsMessage(SnsNotification snsMessage) {

        Objects.requireNonNull(snsMessage, "SNS Message cannot be null.");
        this.snsMessage = snsMessage;
    }

    @Override
    public CompletionStage<Void> ack() {
        //Acknowledgment is handled automatically by AWS SDK.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public T getPayload() {
        return snsMessage == null ? null : (T) snsMessage.getMessage();
    }

    public String getSubject() {
        return snsMessage.getSubject();
    }
}
