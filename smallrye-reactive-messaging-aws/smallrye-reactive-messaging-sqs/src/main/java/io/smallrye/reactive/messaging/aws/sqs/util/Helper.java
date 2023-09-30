package io.smallrye.reactive.messaging.aws.sqs.util;

import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsMessageMetadata;
import io.smallrye.reactive.messaging.json.JsonMapping;

public class Helper {

    public static <M extends SqsMessageMetadata> String serialize(SqsMessage<?, M> message, JsonMapping jsonMapping) {
        if (message.getPayload() instanceof String) {
            return (String) message.getPayload();
        } else {
            return jsonMapping.toJson(message.getPayload());
        }
    }
}
