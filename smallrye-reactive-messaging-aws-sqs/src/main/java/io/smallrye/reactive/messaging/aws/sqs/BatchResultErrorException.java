package io.smallrye.reactive.messaging.aws.sqs;

import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;

/**
 * Exception thrown when a send message batch result contains an error.
 *
 * @see BatchResultErrorEntry
 */
public class BatchResultErrorException extends Exception {

    public BatchResultErrorException(BatchResultErrorEntry entry) {
        super("BatchResultError " + entry.code() + " " + entry.message() + ", senderFault = " + entry.senderFault());
    }

}
