package io.smallrye.reactive.messaging.rabbitmq.og.reply;

public abstract class CorrelationId {

    public abstract String toString();

    public abstract int hashCode();

    public abstract boolean equals(Object o);
}
