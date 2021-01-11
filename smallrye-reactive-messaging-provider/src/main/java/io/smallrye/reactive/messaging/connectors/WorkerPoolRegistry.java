package io.smallrye.reactive.messaging.connectors;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;
import static io.smallrye.reactive.messaging.i18n.ProviderMessages.msg;

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
import io.smallrye.reactive.messaging.helpers.Validation;
import io.vertx.mutiny.core.WorkerExecutor;

@ApplicationScoped
public class WorkerPoolRegistry {
    private static final String WORKER_CONFIG_PREFIX = "smallrye.messaging.worker";
    private static final String WORKER_CONCURRENCY = "max-concurrency";

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    private Instance<Config> configInstance;

    private final Map<String, Integer> workerConcurrency = new HashMap<>();
    private final Map<String, WorkerExecutor> workerExecutors = new ConcurrentHashMap<>();

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(100) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (!workerExecutors.isEmpty()) {
            for (WorkerExecutor executor : workerExecutors.values()) {
                executor.close();
            }
        }
    }

    public <T> Uni<T> executeWork(Uni<T> uni, String workerName, boolean ordered) {
        Objects.requireNonNull(uni, msg.actionNotProvided());

        if (workerName == null) {
            return executionHolder.vertx().executeBlocking(uni, ordered);
        } else {
            return getWorker(workerName).executeBlocking(uni, ordered);
        }
    }

    private WorkerExecutor getWorker(String workerName) {
        Objects.requireNonNull(workerName, msg.workerNameNotSpecified());

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
                        log.workerPoolCreated(workerName, workerConcurrency.get(workerName));
                        workerExecutors.put(workerName, executor);
                    }
                }
            }
            if (executor != null) {
                return executor;
            } else {
                throw ex.runtimeForFailedWorker(workerName);
            }
        }

        // Shouldn't get here
        throw ex.illegalArgumentForFailedWorker();
    }

    public <T> void analyzeWorker(AnnotatedType<T> annotatedType) {
        Objects.requireNonNull(annotatedType, msg.annotatedTypeWasEmpty());

        Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

        methods.stream()
                .filter(m -> m.isAnnotationPresent(Blocking.class))
                .forEach(m -> defineWorker(m.getJavaMember()));
    }

    public void defineWorker(String className, String method, String poolName) {
        Objects.requireNonNull(className, msg.classNameWasEmpty());
        Objects.requireNonNull(method, msg.methodWasEmpty());

        if (!poolName.equals(Blocking.DEFAULT_WORKER_POOL)) {
            // Validate @Blocking value is not empty, if set
            if (Validation.isBlank(poolName)) {
                throw ex.illegalArgumentForAnnotationNullOrBlank("@Blocking", className + "#" + method);
            }

            // Validate @Blocking worker pool has configuration to define concurrency
            String workerConfigKey = WORKER_CONFIG_PREFIX + "." + poolName + "." + WORKER_CONCURRENCY;
            Optional<Integer> concurrency = configInstance.get().getOptionalValue(workerConfigKey, Integer.class);
            if (!concurrency.isPresent()) {
                throw ex.illegalArgumentForWorkerConfigKey("@Blocking", className + "#" + method,
                        workerConfigKey);
            }

            workerConcurrency.put(poolName, concurrency.get());
        }
    }

    private void defineWorker(Method method) {
        Objects.requireNonNull(method, msg.methodWasEmpty());

        Blocking blocking = method.getAnnotation(Blocking.class);

        String methodName = method.getName();
        String className = method.getDeclaringClass().getName();

        // Validate @Blocking is used in conjunction with @Incoming, or @Outgoing
        if (!(method.isAnnotationPresent(Incoming.class) || method.isAnnotationPresent(Outgoing.class))) {
            throw ex.illegalBlockingSignature(className + "#" + method);
        }

        defineWorker(className, methodName, blocking.value());
    }

}
