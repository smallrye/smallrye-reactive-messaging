package io.smallrye.reactive.messaging.aws.sqs;

import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Implementations of this interface can be used to customize the {@link ReceiveMessageRequest} before it is sent to the SQS
 * service.
 * CDI beans that implement this interface will be discovered using the {@link io.smallrye.common.annotation.Identifier}
 * qualifier,
 * matched against the configuration or the channel name.
 */
public interface SqsReceiveMessageRequestCustomizer {

    void customize(ReceiveMessageRequest.Builder builder);
}
