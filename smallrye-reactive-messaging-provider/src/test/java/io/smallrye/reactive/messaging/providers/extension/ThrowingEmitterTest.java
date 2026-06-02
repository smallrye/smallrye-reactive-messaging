package io.smallrye.reactive.messaging.providers.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.MultiEmitter;

class ThrowingEmitterTest {

    @Test
    void testRequestedReturnsBufferSize() {
        AtomicReference<MultiEmitter<? super String>> emitterRef = new AtomicReference<>();

        ThrowingEmitter.<String> create(emitterRef::set, 1)
                .subscribe().withSubscriber(new AssertSubscriber<>(0));

        MultiEmitter<? super String> emitter = emitterRef.get();
        assertThat(emitter).isNotNull();
        assertThat(emitter.requested()).isEqualTo(1);

        emitter.emit("a");
        assertThat(emitter.requested()).isEqualTo(0);
    }

    @Test
    void testRequestedTracksDownstreamRequests() {
        AtomicReference<MultiEmitter<? super String>> emitterRef = new AtomicReference<>();

        ThrowingEmitter.<String> create(emitterRef::set, 0)
                .subscribe().withSubscriber(new AssertSubscriber<>(1));

        MultiEmitter<? super String> emitter = emitterRef.get();
        assertThat(emitter).isNotNull();
        assertThat(emitter.requested()).isEqualTo(1);

        emitter.emit("a");
        assertThat(emitter.requested()).isEqualTo(0);
    }

    @Test
    void testEmitThrowsWhenNoRequests() {
        AtomicReference<MultiEmitter<? super String>> emitterRef = new AtomicReference<>();

        ThrowingEmitter.<String> create(emitterRef::set, 1)
                .subscribe().withSubscriber(new AssertSubscriber<>(0));

        MultiEmitter<? super String> emitter = emitterRef.get();

        emitter.emit("a");
        assertThatThrownBy(() -> emitter.emit("b"))
                .isInstanceOf(IllegalStateException.class);
    }
}
