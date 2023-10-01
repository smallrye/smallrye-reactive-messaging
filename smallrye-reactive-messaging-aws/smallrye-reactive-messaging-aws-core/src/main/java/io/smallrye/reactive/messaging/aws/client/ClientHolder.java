package io.smallrye.reactive.messaging.aws.client;

import io.smallrye.reactive.messaging.aws.serialization.Deserializer;
import io.smallrye.reactive.messaging.aws.serialization.Serializer;
import io.vertx.mutiny.core.Vertx;

public class ClientHolder<CLIENT, CONFIG> {

    private final CLIENT client;
    private final Vertx vertx;
    private final CONFIG config;
    private final Serializer serializer;
    private final Deserializer deserializer;

    public ClientHolder(CLIENT client, Vertx vertx, CONFIG config, Serializer serializer, Deserializer deserializer) {
        this.client = client;
        this.vertx = vertx;
        this.config = config;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public CLIENT getClient() {
        return client;
    }

    public Vertx getVertx() {
        return vertx;
    }

    public CONFIG getConfig() {
        return config;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public Deserializer getDeserializer() {
        return deserializer;
    }
}
