package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.*;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;

/**
 * Class responsible for managing mediators
 */
@ApplicationScoped
public class MediatorManager {

    public static final String STRICT_MODE_PROPERTY = "smallrye-messaging-strict-binding";

    private final CollectedMediatorMetadata collected = new CollectedMediatorMetadata();

    @Inject
    MediatorFactory mediatorFactory;

    @Inject
    BeanManager beanManager;

    @Inject
    WorkerPoolRegistry workerPoolRegistry;

    @Inject
    Instance<PublisherDecorator> decorators;

    @Inject
    Instance<MessageConverter> converters;

    @Inject
    HealthCenter health;

    public MediatorManager() {
        boolean strictMode = Boolean.parseBoolean(System.getProperty(STRICT_MODE_PROPERTY, "false"));
        if (strictMode) {
            log.strictModeEnabled();
        }
    }

    public List<MediatorConfiguration> getConfigurations() {
        return collected.mediators();
    }

    public <T> void analyze(AnnotatedType<T> annotatedType, Bean<T> bean) {
        log.scanningType(annotatedType.getJavaClass());
        Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

        methods.stream()
                .filter(this::hasMediatorAnnotations)
                .forEach(method -> {
                    if (shouldCollectMethod(method, collected)) {
                        collected.add(method.getJavaMember(), bean);
                    }
                });
    }

    /**
     * Checks if the given method is not an overloaded version of another method already included.
     */
    private <T> boolean shouldCollectMethod(AnnotatedMethod<? super T> method, CollectedMediatorMetadata collected) {
        // TODO Not very happy with this - it eliminates methods based on name and not full signature.
        Optional<MediatorConfiguration> existing = collected.mediators().stream()
                .filter(mc -> mc.getMethod().getDeclaringClass() == method.getJavaMember().getDeclaringClass()
                        && mc.getMethod().getName().equals(method.getJavaMember().getName()))
                .findAny();
        return !existing.isPresent();
    }

    private <T> boolean hasMediatorAnnotations(AnnotatedMethod<? super T> method) {
        return method.isAnnotationPresent(Incomings.class) || method.isAnnotationPresent(Incoming.class)
                || method.isAnnotationPresent(Outgoing.class);
    }

    private boolean hasMediatorAnnotations(Method m) {
        return m.isAnnotationPresent(Incomings.class) || m.isAnnotationPresent(Incoming.class)
                || m.isAnnotationPresent(Outgoing.class);
    }

    @SuppressWarnings("unused")
    public <T> void analyze(Class<?> beanClass, Bean<T> bean) {
        Class<?> current = beanClass;
        while (current != Object.class) {
            Arrays.stream(current.getDeclaredMethods())
                    .filter(this::hasMediatorAnnotations)
                    .forEach(m -> collected.add(m, bean));

            current = current.getSuperclass();
        }
    }

    public void addAnalyzed(Collection<? extends MediatorConfiguration> mediators) {
        collected.addAll(mediators);
    }

    public AbstractMediator createMediator(MediatorConfiguration configuration) {
        AbstractMediator mediator = mediatorFactory.create(configuration);
        mediator.setDecorators(decorators);
        mediator.setConverters(converters);
        mediator.setHealth(health);
        mediator.setWorkerPoolRegistry(workerPoolRegistry);

        try {
            Object beanInstance = beanManager.getReference(configuration.getBean(), Object.class,
                    beanManager.createCreationalContext(configuration.getBean()));

            if (configuration.getInvokerClass() != null) {
                try {
                    Constructor<? extends Invoker> constructorUsingBeanInstance = configuration.getInvokerClass()
                            .getConstructor(Object.class);
                    if (constructorUsingBeanInstance != null) {
                        mediator.setInvoker(constructorUsingBeanInstance.newInstance(beanInstance));
                    } else {
                        mediator.setInvoker(configuration.getInvokerClass().getDeclaredConstructor()
                                .newInstance());
                    }

                } catch (InstantiationException | IllegalAccessException e) {
                    log.unableToCreateInvoker(configuration.getInvokerClass(), e);
                    throw e;
                }
            }

            mediator.initialize(beanInstance);
        } catch (Throwable e) {
            log.unableToInitializeMediator(mediator.getMethodAsString(), e);
            throw new DefinitionException(e);
        }
        return mediator;
    }
}
