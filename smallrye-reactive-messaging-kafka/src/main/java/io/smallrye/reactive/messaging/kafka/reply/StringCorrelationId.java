package io.smallrye.reactive.messaging.kafka.reply;

import java.util.Objects;

public class StringCorrelationId extends CorrelationId {

    private final String id;

    public StringCorrelationId(String id) {
        this.id = id;
    }

    public static StringCorrelationId fromBytes(byte[] bytes) {
        return new StringCorrelationId(new String(bytes));
    }

    @Override
    public byte[] toBytes() {
        return id.getBytes();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StringCorrelationId that = (StringCorrelationId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return id;
    }
}
