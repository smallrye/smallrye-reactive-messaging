package io.smallrye.reactive.messaging.eventbus.codec;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.Json;

public class PersonCodec implements MessageCodec<Person, Person> {
    @Override
    public void encodeToWire(Buffer buffer, Person person) {
        buffer.appendBuffer(Json.encodeToBuffer(person));
    }

    @Override
    public Person decodeFromWire(int pos, Buffer buffer) {
        return buffer.toJsonObject().mapTo(Person.class);
    }

    @Override
    public Person transform(Person person) {
        return person;
    }

    @Override
    public String name() {
        return "PersonCodec";
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
