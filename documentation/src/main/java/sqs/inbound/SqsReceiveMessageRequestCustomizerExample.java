package sqs.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.aws.sqs.SqsReceiveMessageRequestCustomizer;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

@Identifier("my-customizer") // or with channel name @Identifier("data")
@ApplicationScoped
public class SqsReceiveMessageRequestCustomizerExample implements SqsReceiveMessageRequestCustomizer {
    @Override
    public void customize(ReceiveMessageRequest.Builder builder) {
        builder.visibilityTimeout(10)
                .messageAttributeNames("my-attribute");
    }
}
