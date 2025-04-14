package io.smallrye.reactive.messaging.providers.connectors;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.AnnotatedMethod;
import jakarta.enterprise.inject.spi.AnnotatedType;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.providers.helpers.Validation;
import io.vertx.core.impl.ContextInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.WorkerExecutor;

@ApplicationScoped
public class WorkerPoolRegistry {
    public static final String WORKER_CONFIG_PREFIX = "smallrye.messaging.worker";
    public static final String WORKER_CONCURRENCY = "max-concurrency";

    @Inject
    Instance<ExecutionHolder> executionHolder;

    @Inject
    Instance<Config> configInstance;

    private final Map<String, Integer> workerConcurrency = new HashMap<>();
    private final Map<String, WorkerExecutor> workerExecutors = new ConcurrentHashMap<>();
    private ExecutionHolder holder;

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(100) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (!workerExecutors.isEmpty()) {
            for (WorkerExecutor executor : workerExecutors.values()) {
                if (Infrastructure.canCallerThreadBeBlocked()) {
                    executor.closeAndAwait();
                } else {
                    executor.closeAndForget();
                }
            }
        }
    }

    @PostConstruct
    public void init() {
        if (executionHolder.isUnsatisfied()) {
            log.noExecutionHolderDisablingBlockingSupport();
        } else {
            this.holder = executionHolder.get();
        }
    }

    public <T> Uni<T> executeWork(Context msgContext, Uni<T> uni, String workerName, boolean ordered) {
        if (holder == null) {
            throw new UnsupportedOperationException("@Blocking disabled");
        }
        Objects.requireNonNull(uni, msg.actionNotProvided());
        if (workerName == null) {
            if (msgContext != null) {
                return msgContext.executeBlocking(uni, ordered);
            }
            // No current context, use the Vert.x instance.
            return holder.vertx().executeBlocking(uni, ordered);
        } else {
            WorkerExecutor worker = getWorker(workerName);
            if (msgContext != null) {
                return uniOnMessageContext(worker.executeBlocking(uni, ordered), msgContext)
                        .onItemOrFailure().transformToUni((item, failure) -> {
                            return Uni.createFrom().emitter(emitter -> {
                                if (failure != null) {
                                    msgContext.runOnContext(() -> emitter.fail(failure));
                                } else {
                                    msgContext.runOnContext(() -> emitter.complete(item));
                                }
                            });
                        });
            }
            return worker.executeBlocking(uni, ordered);
        }
    }

    private static <T> Uni<T> uniOnMessageContext(Uni<T> uni, Context msgContext) {
        if (msgContext != null && !msgContext.equals(Vertx.currentContext())) {
            return Uni.createFrom().deferred(() -> uni)
                    .runSubscriptionOn(r -> new ContextPreservingRunnable(r, msgContext).run());
        }
        return uni;
    }

    private static final class ContextPreservingRunnable implements Runnable {

        private final Runnable task;
        private final io.vertx.core.Context context;

        public ContextPreservingRunnable(Runnable task, Context context) {
            this.task = task;
            this.context = context.getDelegate();
        }

        @Override
        public void run() {
            if (context instanceof ContextInternal) {
                ContextInternal contextInternal = (ContextInternal) context;
                final var previousContext = contextInternal.beginDispatch();
                try {
                    task.run();
                } finally {
                    contextInternal.endDispatch(previousContext);
                }
            } else {
                task.run();
            }
        }
    }

    public WorkerExecutor getWorker(String workerName) {
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
                        executor = holder.vertx().createSharedWorkerExecutor(workerName,
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
