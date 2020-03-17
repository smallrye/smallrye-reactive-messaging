package io.smallrye.reactive.messaging.http;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.http.converters.Serializer;
import io.vertx.core.json.Json;
import io.vertx.mutiny.core.buffer.Buffer;

public class PersonSerializer extends Serializer<Person> {
    @Override
    public Uni<Buffer> convert(Person payload) {
        return Uni.createFrom().item(Buffer.buffer().appendString(Json.encode(payload)));
    }

    @Override
    public Class<? extends Person> input() {
        return Person.class;
    }
}
