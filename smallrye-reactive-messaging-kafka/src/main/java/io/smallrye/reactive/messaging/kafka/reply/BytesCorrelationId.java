package io.smallrye.reactive.messaging.kafka.reply;

import java.util.Arrays;
import java.util.Base64;

public class BytesCorrelationId extends CorrelationId {

    private final byte[] bytes;

    public BytesCorrelationId(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] toBytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BytesCorrelationId that = (BytesCorrelationId) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public String toString() {
        return Base64.getEncoder().encodeToString(bytes);
    }
}
