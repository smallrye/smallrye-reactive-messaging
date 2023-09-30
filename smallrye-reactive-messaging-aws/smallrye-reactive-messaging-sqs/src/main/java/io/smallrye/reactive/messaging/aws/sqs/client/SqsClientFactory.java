package io.smallrye.reactive.messaging.aws.sqs.client;

import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorOutgoingConfiguration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class SqsClientFactory {

    public static SqsAsyncClient createSqsClient(final SqsConnectorIncomingConfiguration config) {
        return null;
    }

    public static SqsAsyncClient createSqsClient(final SqsConnectorOutgoingConfiguration config) {
        return null;
    }
}
