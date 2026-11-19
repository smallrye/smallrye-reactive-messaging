package io.smallrye.reactive.messaging.providers.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.PausableChannel;

class CompositePausableChannelTest {

    @Test
    void pauseAndResumeApplyToEveryPart() {
        FakePausable p1 = new FakePausable(Duration.ofSeconds(1));
        FakePausable p2 = new FakePausable(Duration.ofSeconds(1));
        CompositePausableChannel composite = new CompositePausableChannel(List.of(p1, p2));

        assertThat(composite.isPaused()).isFalse();
        composite.pause();
        assertThat(p1.isPaused()).isTrue();
        assertThat(p2.isPaused()).isTrue();
        assertThat(composite.isPaused()).isTrue();

        composite.resume();
        assertThat(p1.isPaused()).isFalse();
        assertThat(p2.isPaused()).isFalse();
    }

    @Test
    void isPausedOnlyWhenEveryPartIsPaused() {
        FakePausable p1 = new FakePausable(Duration.ofSeconds(1));
        FakePausable p2 = new FakePausable(Duration.ofSeconds(1));
        CompositePausableChannel composite = new CompositePausableChannel(List.of(p1, p2));

        p1.pause();
        assertThat(composite.isPaused()).isFalse();
    }

    @Test
    void pauseAndDrainCompletesWhenAllPartsAreDrained() {
        FakePausable p1 = new FakePausable(Duration.ofSeconds(1));
        FakePausable p2 = new FakePausable(Duration.ofSeconds(1));
        CompositePausableChannel composite = new CompositePausableChannel(List.of(p1, p2));

        CompletableFuture<Void> drain = composite.pauseAndDrain().subscribeAsCompletionStage();
        assertThat(p1.isPaused()).isTrue();
        assertThat(p2.isPaused()).isTrue();
        assertThat(drain).isNotDone();

        p1.drained.complete(null);
        assertThat(drain).isNotDone();

        p2.drained.complete(null);
        assertThat(drain).isCompleted();
    }

    @Test
    void drainTimeoutIsTheLongestOfTheParts() {
        CompositePausableChannel composite = new CompositePausableChannel(List.of(
                new FakePausable(Duration.ofSeconds(5)),
                new FakePausable(Duration.ofSeconds(20)),
                new FakePausable(Duration.ofSeconds(10))));

        assertThat(composite.getDrainTimeout()).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    void inFlightCountIsUnknownWhenAPartDoesNotReportIt() {
        CompositePausableChannel composite = new CompositePausableChannel(List.of(new FakePausable(Duration.ZERO)));

        assertThat(composite.getInFlightCount()).isEqualTo(-1);
        assertThat(CompositePausableChannel.inFlightCount(composite)).isEqualTo(-1);
    }

    @Test
    void needsAtLeastOnePart() {
        assertThatThrownBy(() -> new CompositePausableChannel(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static class FakePausable implements PausableChannel {

        private final Duration drainTimeout;
        private volatile boolean paused;
        final CompletableFuture<Void> drained = new CompletableFuture<>();

        FakePausable(Duration drainTimeout) {
            this.drainTimeout = drainTimeout;
        }

        @Override
        public boolean isPaused() {
            return paused;
        }

        @Override
        public void pause() {
            paused = true;
        }

        @Override
        public void resume() {
            paused = false;
        }

        @Override
        public Uni<Void> pauseAndDrain() {
            pause();
            return Uni.createFrom().completionStage(drained);
        }

        @Override
        public Duration getDrainTimeout() {
            return drainTimeout;
        }
    }
}
