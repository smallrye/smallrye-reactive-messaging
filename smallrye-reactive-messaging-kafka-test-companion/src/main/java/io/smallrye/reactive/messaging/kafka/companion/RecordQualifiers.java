package io.smallrye.reactive.messaging.kafka.companion;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Qualifier producers to compose using {@link Multi#plug} functions
 */
public class RecordQualifiers {

    private RecordQualifiers() {
    }

    public static <T> Function<Multi<T>, Multi<T>> until(Long first, Duration duration, Predicate<T> untilPredicate) {
        return m -> {
            Multi<T> input = m;
            if (first != null) {
                input = input.plug(until(first));
            }
            if (duration != null) {
                input = input.plug(until(duration));
            }
            if (untilPredicate != null) {
                input = input.plug(until(untilPredicate));
            }
            return input;
        };
    }

    public static <T> Function<Multi<T>, Multi<T>> until(Long first) {
        return m -> m.select().first(first);
    }

    public static <T> Function<Multi<T>, Multi<T>> until(Duration duration) {
        return m -> m.select().first(duration);
    }

    public static <T> Function<Multi<T>, Multi<T>> until(Predicate<T> untilPredicate) {
        return m -> m.select().first(untilPredicate.negate());
    }

    public static <T> Function<Multi<T>, Multi<T>> withCallback(Consumer<T> callback, int parallelism) {
        Objects.requireNonNull(callback, "callback function");
        return m -> {
            if (parallelism > 1) {
                return Multi.createFrom()
                        .resource(() -> Executors.newFixedThreadPool(parallelism),
                                e -> m.onItem().transformToUniAndMerge(
                                        cr -> Uni.createFrom().item(cr).invoke(callback).runSubscriptionOn(e)))
                        .withFinalizer(ExecutorService::shutdown);
            } else {
                return m.onItem().invoke(callback);
            }
        };
    }

    public static <T> Function<Multi<T>, Multi<T>> withCallback(Consumer<T> callback) {
        return withCallback(callback, 1);
    }
}
