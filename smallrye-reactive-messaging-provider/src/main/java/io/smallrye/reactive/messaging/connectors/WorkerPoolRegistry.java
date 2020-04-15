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
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
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

    private Map<String, Integer> workerConcurrency = new HashMap<>();
    private Map<String, WorkerExecutor> workerExecutors = new ConcurrentHashMap<>();

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(100) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (!workerExecutors.isEmpty()) {
            for (WorkerExecutor executor : workerExecutors.values()) {
                executor.close();
            }
        }
    }

    public <T> Uni<T> executeWork(Handler<Promise<T>> blockingCodeHandler, String workerName, boolean ordered) {
        Objects.requireNonNull(blockingCodeHandler, "Action to execute not provided");

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
        if (workerConcurrency.containsKey(workerName)) {
            WorkerExecutor executor = workerExecutors.get(workerName);
            if (executor == null) {
                synchronized (this) {
                    executor = workerExecutors.get(workerName);
                    if (executor == null) {
                        executor = executionHolder.vertx().createSharedWorkerExecutor(workerName,
                                workerConcurrency.get(workerName));
                        LoggerFactory.getLogger(WorkerPoolRegistry.class)
                                .info("Created worker pool named " + workerName + " with concurrency of "
                                        + workerConcurrency.get(workerName));
                        workerExecutors.put(workerName, executor);
                    }
                }
            }
            if (executor != null) {
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
                .forEach(m -> defineWorker(m.getJavaMember()));
    }

    public void defineWorker(String className, String method, String poolName) {
        Objects.requireNonNull(className, "className was empty");
        Objects.requireNonNull(method, "Method was empty");

        if (!poolName.equals(Blocking.DEFAULT_WORKER_POOL)) {
            // Validate @Blocking value is not empty, if set
            if (Validation.isBlank(poolName)) {
                throw getBlockingError(className, method, "value is blank or null");
            }

            // Validate @Blocking worker pool has configuration to define concurrency
            String workerConfigKey = WORKER_CONFIG_PREFIX + "." + poolName + "." + WORKER_CONCURRENCY;
            Optional<Integer> concurrency = ConfigProvider.getConfig().getOptionalValue(workerConfigKey, Integer.class);
            if (!concurrency.isPresent()) {
                throw getBlockingError(className, method, workerConfigKey + " was not defined");
            }

            workerConcurrency.put(poolName, concurrency.get());
        }
    }

    private void defineWorker(Method method) {
        Objects.requireNonNull(method, "Method was empty");

        Blocking blocking = method.getAnnotation(Blocking.class);

        String methodName = method.getName();
        String className = method.getDeclaringClass().getName();

        // Validate @Blocking is used in conjunction with @Incoming, or @Outgoing
        if (!(method.isAnnotationPresent(Incoming.class) || method.isAnnotationPresent(Outgoing.class))) {
            throw getBlockingError(className, methodName, "no @Incoming or @Outgoing present");
        }

        defineWorker(className, methodName, blocking.value());
    }

    private IllegalArgumentException getBlockingError(String className, String method, String message) {
        return new IllegalArgumentException(
                "Invalid method annotated with @Blocking: " + className + "#" + method + " - " + message);
    }
}
