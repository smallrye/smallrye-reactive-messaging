package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.Invoker;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.helpers.BroadcastHelper;
import io.smallrye.reactive.messaging.providers.helpers.ConverterUtils;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

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

    protected <T> Uni<T> invokeOnMessageContext(Message<?> message, Object... args) {
        return LocalContextMetadata.invokeOnMessageContext(message, x -> invoke(args));
    }

    @SuppressWarnings("unchecked")
    protected <T> Uni<T> invokeBlocking(Message<?> message, Object... args) {
        try {
            Optional<LocalContextMetadata> metadata = message != null ? message.getMetadata().get(LocalContextMetadata.class)
                    : Optional.empty();
            Context currentContext = metadata.map(m -> Context.newInstance(m.context()))
                    .orElseGet(Vertx::currentContext);
            return workerPoolRegistry.executeWork(currentContext,
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
        return ConverterUtils.convert(upstream, converters, configuration.getIngestedPayloadType());
    }

}
