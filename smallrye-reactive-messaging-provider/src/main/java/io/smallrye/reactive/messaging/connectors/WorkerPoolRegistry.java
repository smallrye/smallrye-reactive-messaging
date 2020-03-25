package io.smallrye.reactive.messaging.connectors;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.helpers.Validation;
import io.vertx.core.Handler;
import io.vertx.mutiny.core.Promise;
import io.vertx.mutiny.core.WorkerExecutor;

@ApplicationScoped
public class WorkerPoolRegistry {
    private static final String WORKER_CONFIG_PREFIX = "smallrye.messaging.worker";
    private static final String WORKER_CONCURRENCY = "max-concurrency";

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    private Instance<Config> configInstance;

    private Map<String, Integer> workerDefinitions = new HashMap<>();
    private Map<String, WorkerExecutor> workerExecutors = new ConcurrentHashMap<>();

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS)
            @Priority(100)
            @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (!workerExecutors.isEmpty()) {
            for (WorkerExecutor executor : workerExecutors.values()) {
                executor.close();
            }
        }
    }

    public <T> Uni<T> executeWork(Handler<Promise<T>> blockingCodeHandler, String workerName, boolean ordered) {
        Objects.requireNonNull(blockingCodeHandler, "Code to execute not provided");

        if (workerName == null) {
            return executionHolder.vertx().executeBlocking(blockingCodeHandler, ordered);
        } else {
            return getWorker(workerName).executeBlocking(blockingCodeHandler, ordered);
        }
    }

    private WorkerExecutor getWorker(String workerName) {
        Objects.requireNonNull(workerName, "Worker Name not specified");

        if (workerExecutors.containsKey(workerName)) {
            return workerExecutors.get(workerName);
        }
        if (workerDefinitions.containsKey(workerName)) {
            WorkerExecutor executor = executionHolder.vertx().createSharedWorkerExecutor(workerName,
                    workerDefinitions.get(workerName));
            if (executor != null) {
                workerExecutors.put(workerName, executor);
                return executor;
            } else {
                throw new RuntimeException("Failed to create Worker for " + workerName);
            }
        }

        // Shouldn't get here
        throw new IllegalArgumentException("@Blocking referred to invalid worker name.");
    }

    public <T> void analyzeWorker(AnnotatedType<T> annotatedType) {
        Objects.requireNonNull(annotatedType, "AnnotatedType was empty");

        Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

        methods.stream()
                .filter(m -> m.isAnnotationPresent(Blocking.class))
                .forEach(m -> addWorker(m.getJavaMember()));
    }

    private void addWorker(Method method) {
        Objects.requireNonNull(method, "Method was empty");

        Blocking blocking = method.getAnnotation(Blocking.class);

        // Validate @Blocking is used in conjunction with @Incomings, @Incoming, or @Outgoing
        if (!(method.isAnnotationPresent(Incomings.class) || method.isAnnotationPresent(Incoming.class)
                || method.isAnnotationPresent(Outgoing.class))) {
            throw getBlockingError(method, "no @Incomings, @Incoming, or @Outgoing present");
        }

        if (!blocking.value().equals(Blocking.NO_VALUE)) {
            // Validate @Blocking value is not empty, if set
            if (Validation.isBlank(blocking.value())) {
                throw getBlockingError(method, "value is blank or null");
            }

            // Validate @Blocking worker pool has configuration to define concurrency
            String workerConfigKey = WORKER_CONFIG_PREFIX + "." + blocking.value() + "." + WORKER_CONCURRENCY;
            Optional<Integer> concurrency = configInstance.get().getOptionalValue(workerConfigKey, Integer.class);
            if (!concurrency.isPresent()) {
                throw getBlockingError(method, workerConfigKey + " was not defined");
            }

            workerDefinitions.put(blocking.value(), concurrency.get());
        }
    }

    private IllegalArgumentException getBlockingError(Method method, String message) {
        return new IllegalArgumentException(
                "Invalid method annotated with @Blocking: " + methodAsString(method) + " - " + message);
    }

    public String methodAsString(Method method) {
        return method.getDeclaringClass().getName() + "#" + method.getName();
    }
}
