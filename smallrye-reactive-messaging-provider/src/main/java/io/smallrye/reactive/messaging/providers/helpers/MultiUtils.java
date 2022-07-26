package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.MediatorConfiguration;

public class MultiUtils {

    public static <T> Multi<T> createFromGenerator(Supplier<T> supplier) {
        return Multi.createFrom().generator(() -> null, (s, g) -> {
            g.emit(supplier.get());
            return s;
        });
    }

    public static <T> Multi<T> publisher(Flow.Publisher<T> publisher) {
        Flow.Publisher<T> actual = nonNull(publisher, "publisher");
        if (actual instanceof Multi) {
            return (Multi<T>) actual;
        }
        return Multi.createFrom().safePublisher(publisher);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Multi<? extends Message<?>> handlePreProcessingAcknowledgement(Multi<? extends Message<?>> multi,
            MediatorConfiguration configuration) {
        if (configuration.getAcknowledgment() != Acknowledgment.Strategy.PRE_PROCESSING) {
            return multi;
        }
        return multi.plug(stream -> (Multi) stream
                .onItem().transformToUniAndConcatenate(message -> {
                    CompletionStage<Void> ack = message.ack();
                    return Uni.createFrom().completionStage(ack).map(x -> message);
                }));
    }

    @SuppressWarnings({ "unchecked" })
    public static <T, R> Multi<R> via(Multi<T> multi, Flow.Processor<? super T, ? super R> processor) {
        return multi.plug(stream -> Multi.createFrom().deferred(() -> {
            Multi<R> m = (Multi<R>) MultiUtils.publisher(processor);
            stream.subscribe(processor);
            return m;
        }));
    }

    public static <T, R, P> Flow.Subscriber<T> via(Flow.Processor<T, R> processor, Function<Multi<R>, Multi<P>> function) {
        return new MultiSubscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                processor.onSubscribe(subscription);
                MultiUtils.publisher(processor).plug(function).subscribe().with(r -> {
                    // ignore
                });
            }

            @Override
            public void onItem(T item) {
                processor.onNext(item);
            }

            @Override
            public void onFailure(Throwable throwable) {
                processor.onError(throwable);
            }

            @Override
            public void onCompletion() {
                processor.onComplete();
            }
        };
    }

    public static <T, R> Flow.Subscriber<T> via(Function<Multi<T>, Multi<R>> function) {
        return via(NoopProcessor.create(), function);
    }

    public static class NoopProcessor<T> extends AbstractMulti<T> implements Flow.Processor<T, T>, Flow.Subscription {

        private volatile boolean done = false;
        private volatile boolean cancelled = false;

        private volatile Flow.Subscription upstream = null;
        private static final AtomicReferenceFieldUpdater<NoopProcessor, Flow.Subscription> UPSTREAM_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(NoopProcessor.class, Flow.Subscription.class, "upstream");
        private volatile Flow.Subscriber<? super T> downstream = null;
        private static final AtomicReferenceFieldUpdater<NoopProcessor, Flow.Subscriber> DOWNSTREAM_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(NoopProcessor.class, Flow.Subscriber.class, "downstream");

        public static <I> NoopProcessor<I> create() {
            return new NoopProcessor<>();
        }

        private NoopProcessor() {
        }

        @Override
        public void subscribe(MultiSubscriber<? super T> downstream) {
            ParameterValidation.nonNull(downstream, "downstream");
            if (DOWNSTREAM_UPDATER.compareAndSet(this, null, downstream)) {
                if (upstream != null) {
                    downstream.onSubscribe(this);
                }
            } else {
                Subscriptions.fail(downstream, new IllegalStateException("Already subscribed"));
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription upstream) {
            if (isDoneOrCancelled() || !UPSTREAM_UPDATER.compareAndSet(this, null, upstream)) {
                upstream.cancel();
                return;
            }
            Flow.Subscriber<? super T> subscriber = downstream;
            if (subscriber != null) {
                subscriber.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (isDoneOrCancelled()) {
                return;
            }
            Flow.Subscriber<? super T> subscriber = downstream;
            if (subscriber != null) {
                subscriber.onNext(t);
            }
        }

        private boolean isDoneOrCancelled() {
            return done || cancelled;
        }

        @Override
        public void onError(Throwable failure) {
            Objects.requireNonNull(failure);
            if (isDoneOrCancelled()) {
                return;
            }

            this.done = true;
        }

        @Override
        public void onComplete() {
            if (isDoneOrCancelled()) {
                return;
            }
            this.done = true;
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                UPSTREAM_UPDATER.get(this).request(n);
            }
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            this.cancelled = true;
            DOWNSTREAM_UPDATER.getAndSet(this, null);
        }

    }

}
