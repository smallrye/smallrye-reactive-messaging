package io.smallrye.reactive.messaging.kafka.queues;

import org.apache.kafka.clients.consumer.AcknowledgeType;

/**
 * Metadata for specifying the acknowledgment type when using Kafka Share Groups.
 * <p>
 * This metadata can be attached to a message when calling {@code nack()} to control
 * how the share group consumer should handle the rejected message.
 * </p>
 */
public class ShareGroupAcknowledgement {

    private volatile AcknowledgeType acknowledgeType;

    public ShareGroupAcknowledgement(AcknowledgeType acknowledgeType) {
        this.acknowledgeType = acknowledgeType;
    }

    public ShareGroupAcknowledgement() {
    }

    /**
     * Accepts the message, indicating that it has been processed successfully
     */
    public void accept() {
        this.acknowledgeType = AcknowledgeType.ACCEPT;
    }

    /**
     * Releases the message, indicating that it should be reprocessed by another consumer in the share group
     */
    public void release() {
        this.acknowledgeType = AcknowledgeType.RELEASE;
    }

    /**
     * Rejects the message, indicating that it should not be processed and should be discarded
     */
    public void reject() {
        this.acknowledgeType = AcknowledgeType.REJECT;
    }

    /**
     * Creates a ShareGroupAcknowledgeTypeMetadata with the specified acknowledge type.
     *
     * @param acknowledgeType the acknowledgment type
     * @return the metadata instance
     */
    public static ShareGroupAcknowledgement from(AcknowledgeType acknowledgeType) {
        return new ShareGroupAcknowledgement(acknowledgeType);
    }

    public AcknowledgeType getAcknowledgeType() {
        return acknowledgeType;
    }
}
