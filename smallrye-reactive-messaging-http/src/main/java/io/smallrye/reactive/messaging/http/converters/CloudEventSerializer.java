package io.smallrye.reactive.messaging.http.converters;

import io.cloudevents.json.Json;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.cloudevents.CloudEventMessage;
import io.vertx.mutiny.core.buffer.Buffer;

public class CloudEventSerializer extends Serializer<CloudEventMessage> {

    @Override
    public Uni<Buffer> convert(CloudEventMessage payload) {
        return Uni.createFrom().item(Buffer.buffer(Json.encode(payload)));
    }

    @Override
    public Class<? extends CloudEventMessage> input() {
        return CloudEventMessage.class;
    }

}
