package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.reactive.messaging.providers.AbstractMediator.skipContextPropagation;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.operators.MultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;

public class OrderedBlockingOperator<T extends Message<?>> extends MultiOperator<T, Message<Object>> {
    private final Function<Message<?>, Object> invoker;
    private final BlockingPostInvocationHandler handler;
    private final WorkerPoolRegistry workerPoolRegistry;
    private final String workerPoolName;
    private final String methodAsString;

    public OrderedBlockingOperator(Multi<T> upstream,
            Function<Message<?>, Object> invoker,
            BlockingPostInvocationHandler handler,
            WorkerPoolRegistry workerPoolRegistry,
            String workerPoolName,
            String methodAsString) {
        super(upstream);
        this.invoker = invoker;
        this.handler = handler;
        this.workerPoolRegistry = workerPoolRegistry;
        this.workerPoolName = workerPoolName;
        this.methodAsString = methodAsString;
    }

    @Override
    public void subscribe(MultiSubscriber<? super Message<Object>> subscriber) {
        upstream().subscribe().withSubscriber(new OrderedBlockingProcessor(subscriber));
    }

    public class OrderedBlockingProcessor extends MultiOperatorProcessor<T, Message<Object>> {
        private final Queue<T> queue = Queues.createMpscQueue();
        private final AtomicLong demand = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private volatile boolean upstreamComplete;

        OrderedBlockingProcessor(MultiSubscriber<? super Message<Object>> downstream) {
            super(downstream);
        }

        @Override
        public void onItem(T item) {
            queue.offer(item);
            tryDispatch();
        }

        @Override
        public void request(long n) {
            Subscriptions.add(demand, n);
            super.request(n);
            tryDispatch();
        }

        @Override
        public void onCompletion() {
            upstreamComplete = true;
            tryDispatch();
        }

        @Override
        public void onFailure(Throwable failure) {
            queue.clear();
            super.onFailure(failure);
        }

        private void tryDispatch() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            try {
                workerPoolRegistry.executeWork(null,
                        skipContextPropagation(() -> {
                            drain();
                            return Uni.createFrom().voidItem();
                        }), workerPoolName, true)
                        .subscribe().with(x -> {
                        }, t -> {
                            log.methodException(methodAsString, t);
                            downstream.onFailure(t);
                        });
            } catch (Exception t) {
                log.methodException(methodAsString, t);
                downstream.onFailure(t);
            }
        }

        private void drain() {
            int missed = 1;
            for (;;) {
                long emitted = 0;
                long requests = demand.get();

                while (emitted < requests) {
                    if (isCancelled()) {
                        queue.clear();
                        return;
                    }

                    T message = queue.poll();
                    if (message == null) {
                        break;
                    }

                    Object result = null;
                    Throwable error = null;
                    try {
                        result = invoker.apply(message);
                    } catch (Throwable t) {
                        error = t;
                    }

                    try {
                        Message<Object> outgoing = handler.handle(message, result, error)
                                .await().indefinitely();
                        if (outgoing != null) {
                            downstream.onItem(outgoing);
                            emitted++;
                        } else {
                            super.request(1);
                        }
                    } catch (Throwable t) {
                        queue.clear();
                        downstream.onFailure(t);
                        return;
                    }
                }

                demand.addAndGet(-emitted);

                if (upstreamComplete && queue.isEmpty()) {
                    downstream.onCompletion();
                    return;
                }

                int w = wip.get();
                if (missed == w) {
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                } else {
                    missed = w;
                }
            }
        }
    }

    @FunctionalInterface
    public interface BlockingPostInvocationHandler {
        Uni<? extends Message<Object>> handle(Message<?> message, Object result, Throwable fail);
    }

}
