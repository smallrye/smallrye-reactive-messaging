package io.smallrye.reactive.messaging.providers.locals;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.MultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.providers.PublisherDecorator;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

@ApplicationScoped
public class ContextDecorator implements PublisherDecorator {
    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName) {
        return publisher
                .plug(upstream -> new ContextMulti((Multi<Message<?>>) upstream));
    }

    static class ContextMulti extends MultiOperator<Message<?>, Message<?>> {

        public ContextMulti(Multi<Message<?>> upstream) {
            super(upstream);
        }

        @Override
        public void subscribe(MultiSubscriber<? super Message<?>> subscriber) {
            MultiOperatorProcessor<Message<?>, Message<?>> operator = new ContextProcessor(subscriber);
            upstream().subscribe().withSubscriber(operator);
        }

        static class ContextProcessor extends MultiOperatorProcessor<Message<?>, Message<?>> {

            private volatile Context rootContext;

            private static final AtomicReferenceFieldUpdater<ContextProcessor, Context> ROOT_CONTEXT_UPDATER = AtomicReferenceFieldUpdater
                    .newUpdater(ContextProcessor.class, Context.class, "rootContext");

            public ContextProcessor(MultiSubscriber<? super Message<?>> downstream) {
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
            public void onItem(Message<?> item) {
                Optional<LocalContextMetadata> metadata = item.getMetadata().get(LocalContextMetadata.class);
                if (metadata.isPresent()) {
                    Context context = metadata.get().context();
                    // This make the assumption that ALL the receives message belongs to the same event loop
                    // It's not the case when using multiple Kafka partitions, however this root context is only
                    // used for completion and failure event. As these are terminal events it should not matter.
                    ROOT_CONTEXT_UPDATER.compareAndSet(this, null, VertxContext.getRootContext(context));

                    if (Vertx.currentContext() == context) {
                        // We are on the right context, immediate call
                        super.onItem(item);
                    } else {
                        // Submit the emission on the message context
                        context.runOnContext(ignored -> super.onItem(item));
                    }
                } else {
                    // No stored context, immediate call
                    super.onItem(item);
                }
            }

            @Override
            public void request(long numberOfItems) {
                Context context = Vertx.currentContext();
                if (context != null) {
                    super.request(numberOfItems);
                } else {
                    Context root = ROOT_CONTEXT_UPDATER.get(this);
                    if (root != null) {
                        root.runOnContext(x -> super.request(numberOfItems));
                    } else {
                        super.request(numberOfItems);
                    }
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
