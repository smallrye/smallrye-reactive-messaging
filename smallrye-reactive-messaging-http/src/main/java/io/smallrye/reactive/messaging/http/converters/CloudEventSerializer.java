package io.smallrye.reactive.messaging.http.converters;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.cloudevents.json.Json;
import io.smallrye.reactive.messaging.cloudevents.CloudEventMessage;
import io.vertx.reactivex.core.buffer.Buffer;

public class CloudEventSerializer extends Serializer<CloudEventMessage> {

    @Override
    public CompletionStage<Buffer> convert(CloudEventMessage payload) {
        return CompletableFuture.completedFuture(Buffer.buffer(Json.encode(payload)));
    }

    @Override
    public Class<? extends CloudEventMessage> input() {
        return CloudEventMessage.class;
    }

}
