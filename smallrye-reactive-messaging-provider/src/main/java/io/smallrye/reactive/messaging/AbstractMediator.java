package io.smallrye.reactive.messaging;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import javax.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.helpers.BroadcastHelper;

public abstract class AbstractMediator {

    protected final MediatorConfiguration configuration;
    protected WorkerPoolRegistry workerPoolRegistry;
    private Invoker invoker;
    private Instance<PublisherDecorator> decorators;

    public AbstractMediator(MediatorConfiguration configuration) {
        this.configuration = configuration;
    }

    public synchronized void setInvoker(Invoker invoker) {
        this.invoker = invoker;
    }

    public void setDecorators(Instance<PublisherDecorator> decorators) {
        this.decorators = decorators;
    }

    public void setWorkerPoolRegistry(WorkerPoolRegistry workerPoolRegistry) {
        this.workerPoolRegistry = workerPoolRegistry;
    }

    public void run() {
        // Do nothing by default.
    }

    public void connectToUpstream(PublisherBuilder<? extends Message<?>> publisher) {
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
                        throw new ProcessingException(configuration.methodAsString(), e);
                    }
                };
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke(Object... args) {
        try {
            Objects.requireNonNull(this.invoker, "Invoker not initialized");
            return (T) this.invoker.invoke(args);
        } catch (RuntimeException e) { // NOSONAR
            LoggerFactory.getLogger(configuration().methodAsString())
                    .error("The method " + configuration().methodAsString() + " has thrown an exception", e);
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> Uni<T> invokeBlocking(Object... args) {
        try {
            Objects.requireNonNull(this.invoker, "Invoker not initialized");
            Objects.requireNonNull(this.workerPoolRegistry, "Worker pool not initialized");
            return workerPoolRegistry.executeWork(
                    future -> future.complete((T) this.invoker.invoke(args)),
                    configuration.getWorkerPoolName(),
                    configuration.isBlockingExecutionOrdered());
        } catch (RuntimeException e) {
            LoggerFactory.getLogger(configuration().methodAsString())
                    .error("The method " + configuration().methodAsString() + " has thrown an exception", e);
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

    public PublisherBuilder<? extends Message<?>> getStream() {
        return null;
    }

    public MediatorConfiguration getConfiguration() {
        return configuration;
    }

    public String getMethodAsString() {
        return configuration.methodAsString();
    }

    public SubscriberBuilder<Message<?>, Void> getComputedSubscriber() {
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

    public PublisherBuilder<? extends Message<?>> decorate(PublisherBuilder<? extends Message<?>> input) {
        if (input == null) {
            return null;
        }

        for (PublisherDecorator decorator : decorators) {
            input = decorator.decorate(input, getConfiguration().getOutgoing());
        }

        if (configuration.getBroadcast()) {
            return BroadcastHelper.broadcastPublisher(input.buildRs(), configuration.getNumberOfSubscriberBeforeConnecting());
        } else {
            return input;
        }
    }

}
