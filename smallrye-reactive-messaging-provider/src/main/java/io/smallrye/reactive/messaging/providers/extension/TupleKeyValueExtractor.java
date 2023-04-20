package io.smallrye.reactive.messaging.providers.extension;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.MessageKeyValueExtractor;

@ApplicationScoped
public class TupleKeyValueExtractor implements MessageKeyValueExtractor {

    @Override
    public boolean canExtract(Message<?> in, Type keyType, Type valueType) {
        return in.getPayload() instanceof Tuple2;
    }

    @Override
    public Object extractKey(Message<?> in, Type target) {
        if (in.getPayload() != null) {
            return ((Tuple2) in.getPayload()).getItem1();
        }
        return null;
    }

    @Override
    public Object extractValue(Message<?> in, Type target) {
        if (in.getPayload() != null) {
            return ((Tuple2) in.getPayload()).getItem2();
        }
        return in.getPayload();
    }
}
