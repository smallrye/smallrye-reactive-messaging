package io.smallrye.reactive.messaging;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;
import static io.smallrye.reactive.messaging.i18n.ProviderMessages.msg;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.Prioritized;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.smallrye.reactive.messaging.helpers.BroadcastHelper;
import io.smallrye.reactive.messaging.helpers.TypeUtils;

public abstract class AbstractMediator {

    protected final MediatorConfiguration configuration;
    protected WorkerPoolRegistry workerPoolRegistry;
    private Invoker invoker;
    private Instance<PublisherDecorator> decorators;
    protected HealthCenter health;
    private Instance<MessageConverter> converters;

    public AbstractMediator(MediatorConfiguration configuration) {
        this.configuration = configuration;
    }

    public synchronized void setInvoker(Invoker invoker) {
        this.invoker = invoker;
    }

    public void setDecorators(Instance<PublisherDecorator> decorators) {
        this.decorators = decorators;
    }

    public void setConverters(Instance<MessageConverter> converters) {
        this.converters = converters;
    }

    public void setWorkerPoolRegistry(WorkerPoolRegistry workerPoolRegistry) {
        this.workerPoolRegistry = workerPoolRegistry;
    }

    public void run() {
        // Do nothing by default.
    }

    public void connectToUpstream(Multi<? extends Message<?>> publisher) {
        // Do nothing by default.
    }

    public MediatorConfiguration configuration() {
        return configuration;
    }

    public void initialize(Object bean) {
        // Method overriding initialize MUST call super(bean).
        synchronized (this) {
            if (this.invoker == null) {
                this.invoker = args -> {
                    try {
                        return this.configuration.getMethod().invoke(bean, args);
                    } catch (Exception e) {
                        throw ex.processingException(configuration.methodAsString(), e);
                    }
                };
            }
        }
        Objects.requireNonNull(this.invoker, msg.invokerNotInitialized());
        if (this.configuration.isBlocking()) {
            Objects.requireNonNull(this.workerPoolRegistry, msg.workerPoolNotInitialized());
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke(Object... args) {
        try {
            return (T) this.invoker.invoke(args);
        } catch (RuntimeException e) { // NOSONAR
            log.methodException(configuration().methodAsString(), e);
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> Uni<T> invokeBlocking(Object... args) {
        try {
            return workerPoolRegistry.executeWork(
                    Uni.createFrom().emitter(emitter -> {
                        try {
                            Object result = this.invoker.invoke(args);
                            if (result instanceof CompletionStage) {
                                ((CompletionStage<?>) result).thenAccept(x -> emitter.complete((T) x));
                            } else {
                                emitter.complete((T) result);
                            }
                        } catch (RuntimeException e) {
                            log.methodException(configuration().methodAsString(), e);
                            emitter.fail(e);
                        }
                    }),
                    configuration.getWorkerPoolName(),
                    configuration.isBlockingExecutionOrdered());
        } catch (RuntimeException e) {
            log.methodException(configuration().methodAsString(), e);
            throw e;
        }
    }

    protected CompletionStage<Message<?>> getAckOrCompletion(Message<?> message) {
        CompletionStage<Void> ack = message.ack();
        if (ack != null) {
            return ack.thenApply(x -> message);
        } else {
            return CompletableFuture.completedFuture(message);
        }
    }

    public Multi<? extends Message<?>> getStream() {
        return null;
    }

    public MediatorConfiguration getConfiguration() {
        return configuration;
    }

    public String getMethodAsString() {
        return configuration.methodAsString();
    }

    public Subscriber<Message<?>> getComputedSubscriber() {
        return null;
    }

    public abstract boolean isConnected();

    protected Function<Message<?>, ? extends CompletionStage<? extends Message<?>>> managePreProcessingAck() {
        return this::handlePreProcessingAck;
    }

    protected CompletionStage<Message<?>> handlePreProcessingAck(Message<?> message) {
        if (configuration.getAcknowledgment() == Acknowledgment.Strategy.PRE_PROCESSING) {
            return getAckOrCompletion(message);
        }
        return CompletableFuture.completedFuture(message);
    }

    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> input) {
        if (input == null) {
            return null;
        }

        for (PublisherDecorator decorator : decorators) {
            input = decorator.decorate(input, getConfiguration().getOutgoing());
        }

        if (configuration.getBroadcast()) {
            return BroadcastHelper.broadcastPublisher(input, configuration.getNumberOfSubscriberBeforeConnecting());
        } else {
            return input;
        }
    }

    public void setHealth(HealthCenter health) {
        this.health = health;
    }

    public Multi<? extends Message<?>> convert(Multi<? extends Message<?>> upstream) {
        final Type injectedPayloadType = configuration.getIngestedPayloadType();
        if (injectedPayloadType != null) {
            return upstream
                    .map(new Function<Message<?>, Message<?>>() {

                        MessageConverter actual;

                        @Override
                        public Message<?> apply(Message<?> o) {
                            //noinspection ConstantConditions - it can be `null`
                            if (injectedPayloadType == null) {
                                return o;
                            } else if (o.getPayload() != null && o.getPayload().getClass().equals(injectedPayloadType)) {
                                return o;
                            }

                            if (actual != null) {
                                // Use the cached converter.
                                return actual.convert(o, injectedPayloadType);
                            } else {
                                if (o.getPayload() != null
                                        && TypeUtils.isAssignable(o.getPayload().getClass(), injectedPayloadType)) {
                                    actual = MessageConverter.IdentityConverter.INSTANCE;
                                    return o;
                                }
                                // Lookup and cache
                                for (MessageConverter conv : getSortedConverters()) {
                                    if (conv.canConvert(o, injectedPayloadType)) {
                                        actual = conv;
                                        return actual.convert(o, injectedPayloadType);
                                    }
                                }
                                // No converter found
                                return o;
                            }
                        }
                    });
        }
        return upstream;
    }

    private List<MessageConverter> getSortedConverters() {
        if (converters.isUnsatisfied()) {
            return Collections.emptyList();
        }

        return converters.stream().sorted(new Comparator<MessageConverter>() { // NOSONAR
            @Override
            public int compare(MessageConverter si1, MessageConverter si2) {
                int p1 = 0;
                int p2 = 0;
                if (si1 instanceof Prioritized) {
                    p1 = ((Prioritized) si1).getPriority();
                }
                if (si2 instanceof Prioritized) {
                    p2 = ((Prioritized) si2).getPriority();
                }
                if (si1.equals(si2)) {
                    return 0;
                }
                return Integer.compare(p1, p2);
            }
        }).collect(Collectors.toList());
    }
}
