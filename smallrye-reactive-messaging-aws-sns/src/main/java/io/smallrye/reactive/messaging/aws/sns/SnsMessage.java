package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.aws.sns.i18n.SnsMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.amazonaws.services.sns.message.SnsNotification;

/**
 * Implementation of {@link Message} for SNS. Currently, it only support payload of type {@code String}.
 *
 * @author iabughosh
 */
public class SnsMessage implements Message<String> {

    /**
     * Instance of SNS notification message
     */
    private final SnsNotification snsNotification;
    /**
     * String payload, used for testing
     */
    private final String payload;

    /**
     * Constructor for AWS SNS.
     *
     * @param snsNotification the notification, must not be {@code null}
     */
    public SnsMessage(SnsNotification snsNotification) {
        Objects.requireNonNull(snsNotification, msg.messageNotNull());
        this.snsNotification = snsNotification;
        payload = null;
    }

    /**
     * Constructor for fake SNS.
     *
     * @param payload the payload.
     */
    public SnsMessage(String payload) {
        this.payload = payload;
        snsNotification = null;
    }

    @Override
    public CompletionStage<Void> ack() {
        //Acknowledgment is handled automatically by AWS SDK.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public String getPayload() {
        return snsNotification != null ? snsNotification.getMessage() : payload;
    }

    /**
     * Get message subject of SNS notification.
     *
     * @return message subject.
     */
    public String getSubject() {
        return snsNotification.getSubject();
    }

}
