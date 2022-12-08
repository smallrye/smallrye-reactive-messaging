package io.smallrye.reactive.messaging.providers.locals;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.MultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.core.Context;

/**
 * Decorator to dispatch messages on the Vert.x context attached to the message via {@link LocalContextMetadata}.
 * Low priority to be called before other decorators.
 */
public class ContextOperator {

    public static <T extends Message<?>> Multi<T> apply(Multi<T> publisher) {
        return new ContextMulti<>(publisher);
    }

    static class ContextMulti<T extends Message<?>> extends MultiOperator<T, T> {

        public ContextMulti(Multi<T> upstream) {
            super(upstream);
        }

        @Override
        public void subscribe(MultiSubscriber<? super T> subscriber) {
            MultiOperatorProcessor<T, T> operator = new ContextProcessor<>(subscriber);
            upstream().subscribe().withSubscriber(operator);
        }

        static class ContextProcessor<T extends Message<?>> extends MultiOperatorProcessor<T, T> {

            private volatile Context rootContext;

            private static final AtomicReferenceFieldUpdater<ContextProcessor, Context> ROOT_CONTEXT_UPDATER = AtomicReferenceFieldUpdater
                    .newUpdater(ContextProcessor.class, Context.class, "rootContext");

            public ContextProcessor(MultiSubscriber<? super T> downstream) {
                super(downstream);
            }

            @Override
            public void onFailure(Throwable throwable) {
                // Release context on terminal events
                Context root = ROOT_CONTEXT_UPDATER.getAndSet(this, null);
                // Do NOT check for the current context and trampoline, as we may short-cut the completion event
                // before the processing of the items.
                if (root == null) {
                    super.onFailure(throwable);
                } else {
                    root.runOnContext(ignored -> super.onFailure(throwable));
                }
            }

            @Override
            public void onItem(T item) {
                Optional<LocalContextMetadata> metadata = item.getMetadata().get(LocalContextMetadata.class);
                if (metadata.isPresent()) {
                    Context context = metadata.get().context();
                    // This make the assumption that ALL the receives message belongs to the same event loop
                    // It's not the case when using multiple Kafka partitions, however this root context is only
                    // used for completion and failure event. As these are terminal events it should not matter.
                    ROOT_CONTEXT_UPDATER.compareAndSet(this, null, VertxContext.getRootContext(context));

                    VertxContext.runOnContext(context, () -> super.onItem(item));
                } else {
                    // No stored context, immediate call
                    super.onItem(item);
                }
            }

            @Override
            public void request(long numberOfItems) {
                Context root = ROOT_CONTEXT_UPDATER.get(this);
                if (root != null) {
                    root.runOnContext(x -> super.request(numberOfItems));
                } else {
                    super.request(numberOfItems);
                }
            }

            @Override
            public void onCompletion() {
                // Release context on terminal events
                Context root = ROOT_CONTEXT_UPDATER.getAndSet(this, null);
                // Do NOT check for the current context and trampoline, as we may short-cut the completion event
                // before the processing of the items.
                if (root == null) {
                    super.onCompletion();
                } else {
                    root.runOnContext(ignored -> super.onCompletion());
                }
            }
        }

    }

}
