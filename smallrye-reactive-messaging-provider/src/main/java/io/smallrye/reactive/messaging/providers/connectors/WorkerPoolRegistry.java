package io.smallrye.reactive.messaging.providers.connectors;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

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
import io.vertx.core.impl.WorkerExecutorInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.WorkerExecutor;

@ApplicationScoped
public class WorkerPoolRegistry {
    public static final String WORKER_CONFIG_PREFIX = "smallrye.messaging.worker";
    public static final String WORKER_CONCURRENCY = "max-concurrency";
    public static final String SHUTDOWN_TIMEOUT = "shutdown-timeout";
    public static final String SHUTDOWN_CHECK_INTERVAL = "shutdown-check-interval";
    public static final int DEFAULT_SHUTDOWN_TIMEOUT_MS = 60 * 1000; // 1 minute
    public static final int DEFAULT_SHUTDOWN_CHECK_INTERVAL_MS = 5 * 1000; // 5 seconds

    @Inject
    Instance<ExecutionHolder> executionHolder;

    @Inject
    Instance<Config> configInstance;

    private final Map<String, WorkerPoolConfig> workerConfig = new HashMap<>();
    private final Map<String, WorkerExecutor> workerExecutors = new ConcurrentHashMap<>();
    private ExecutionHolder holder;

    private volatile boolean closed = false;

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(100) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (!workerExecutors.isEmpty()) {
            closed = true;
            // In most cases this is called from the main thread, so we can block.
            if (!Infrastructure.canCallerThreadBeBlocked()) {
                for (WorkerExecutor executor : workerExecutors.values()) {
                    executor.closeAndForget();
                }
                return;
            }
            // Shutdown all worker executors
            for (Map.Entry<String, WorkerExecutor> entry : workerExecutors.entrySet()) {
                ((WorkerExecutorInternal) entry.getValue().getDelegate()).getPool().executor().shutdown();
            }
            long start = System.nanoTime();
            boolean terminated = false;
            int loop = 1;
            long elapsed = 0;
            while (!terminated) {
                terminated = true;
                for (Map.Entry<String, WorkerExecutor> entry : workerExecutors.entrySet()) {
                    WorkerExecutor workerExecutor = entry.getValue();
                    ExecutorService innerExecutor = ((WorkerExecutorInternal) workerExecutor.getDelegate()).getPool()
                            .executor();
                    WorkerPoolConfig poolConfig = workerConfig.get(entry.getKey());
                    long timeout = poolConfig.shutdownTimeout().toNanos();
                    long interval = poolConfig.shutdownCheckInterval().toNanos();
                    log.debugf("Await termination loop: %s, remaining: %s", loop++, timeout - elapsed);
                    try {
                        if (!innerExecutor.awaitTermination(Math.min(timeout, interval), NANOSECONDS)) {
                            elapsed = System.nanoTime() - start;
                            if (elapsed >= timeout) {
                                innerExecutor.shutdownNow();
                                workerExecutor.closeAndAwait();
                            } else {
                                terminated = false; // Still waiting
                            }
                        }
                    } catch (InterruptedException ignored) {
                        terminated = false;
                    }
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
        if (closed) {
            throw new RejectedExecutionException("WorkerPoolRegistry is being shut down");
        }
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
        if (workerConfig.containsKey(workerName)) {
            WorkerExecutor executor = workerExecutors.get(workerName);
            if (executor == null) {
                synchronized (this) {
                    executor = workerExecutors.get(workerName);
                    if (executor == null) {
                        int poolSize = workerConfig.get(workerName).maxConcurrency();
                        executor = holder.vertx().createSharedWorkerExecutor(workerName, poolSize);
                        log.workerPoolCreated(workerName, poolSize);
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

            workerConfig.put(poolName, getWorkerPoolConfig(className, method, poolName));
        }
    }

    private WorkerPoolConfig getWorkerPoolConfig(String className, String method, String poolName) {
        // Validate @Blocking worker pool has configuration to define concurrency
        Config config = configInstance.get();
        String maxConcurrencyConfigKey = WORKER_CONFIG_PREFIX + "." + poolName + "." + WORKER_CONCURRENCY;
        String shutdownTimeoutConfigKey = WORKER_CONFIG_PREFIX + "." + poolName + "." + SHUTDOWN_TIMEOUT;
        String shutdownCheckIntervalConfigKey = WORKER_CONFIG_PREFIX + "." + poolName + "." + SHUTDOWN_CHECK_INTERVAL;
        int maxConcurrency = config.getOptionalValue(maxConcurrencyConfigKey, Integer.class)
                .orElseThrow(() -> ex.illegalArgumentForWorkerConfigKey("@Blocking", className + "#" + method,
                        maxConcurrencyConfigKey));
        int shutdownTimeout = config.getOptionalValue(shutdownTimeoutConfigKey, Integer.class)
                .orElse(DEFAULT_SHUTDOWN_TIMEOUT_MS);
        int shutdownCheckInterval = config.getOptionalValue(shutdownCheckIntervalConfigKey, Integer.class)
                .orElse(DEFAULT_SHUTDOWN_CHECK_INTERVAL_MS);
        return new WorkerPoolConfig(maxConcurrency, shutdownTimeout, shutdownCheckInterval);
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
