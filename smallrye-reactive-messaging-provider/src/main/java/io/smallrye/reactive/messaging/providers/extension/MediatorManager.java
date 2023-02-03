package io.smallrye.reactive.messaging.providers.extension;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.*;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.*;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.providers.AbstractMediator;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.wiring.Graph;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;

/**
 * Class responsible for creating mediators instances and starting the management.
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
    // @Any would only be needed if we wanted to allow implementations with qualifiers
    Instance<PublisherDecorator> decorators;

    @Inject
    // @Any would only be needed if we wanted to allow implementations with qualifiers
    Instance<SubscriberDecorator> subscriberDecorators;

    @Inject
    // @Any would only be needed if we wanted to allow implementations with qualifiers
    Instance<MessageConverter> converters;

    private final List<EmitterConfiguration> emitters = new ArrayList<>();

    @Inject
    HealthCenter health;
    private final List<ChannelConfiguration> channels = new ArrayList<>();
    @Inject
    ChannelRegistry registry;
    @Inject
    Wiring wiring;
    @Inject
    // @Any would only be needed if we wanted to allow implementations with qualifiers
    Instance<ChannelRegistar> registars;

    @Inject
    @ConfigProperty(name = STRICT_MODE_PROPERTY, defaultValue = "false")
    boolean strictMode;

    public <T> void analyze(AnnotatedType<T> annotatedType, Bean<T> bean) {

        if (strictMode) {
            collected.strict();
        }

        log.scanningType(annotatedType.getJavaClass());
        Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

        methods.stream()
                .filter(this::hasMediatorAnnotations)
                .forEach(method -> {
                    if (shouldCollectMethod(method.getJavaMember(), collected)) {
                        collected.add(method.getJavaMember(), bean);
                    }
                });
    }

    /**
     * This method is used in the Quarkus extension.
     *
     * @param beanClass the bean class
     * @param bean the bean instance
     * @param <T> the type.
     */
    @SuppressWarnings("unused")
    public <T> void analyze(Class<?> beanClass, Bean<T> bean) {
        Class<?> current = beanClass;
        while (current != Object.class) {
            Arrays.stream(current.getDeclaredMethods())
                    .filter(this::hasMediatorAnnotations)
                    .forEach(method -> {
                        if (shouldCollectMethod(method, collected)) {
                            collected.add(method, bean);
                        }
                    });
            current = current.getSuperclass();
        }
    }

    @SuppressWarnings("unused")
    public void addAnalyzed(Collection<? extends MediatorConfiguration> mediators) {
        collected.addAll(mediators);
    }

    public void addEmitter(EmitterConfiguration emitterConfiguration) {
        emitters.add(emitterConfiguration);
    }

    public void addChannel(ChannelConfiguration channel) {
        channels.add(channel);
    }

    /**
     * Checks if the given method is not an overloaded version of another method already included.
     */
    private boolean shouldCollectMethod(Method method, CollectedMediatorMetadata collected) {
        Optional<MediatorConfiguration> existing = collected.mediators().stream()
                .filter(mc -> mc.getMethod().getDeclaringClass() == method.getDeclaringClass()
                        && mc.getMethod().getName().equals(method.getName())
                        && areParametersEquivalent(method, mc.getMethod()))
                .findAny();
        return !existing.isPresent();
    }

    private boolean areParametersEquivalent(Method method1, Method method2) {
        if (method1.getParameterCount() != method2.getParameterCount()) {
            return false;
        }
        for (int i = 0; i < method1.getParameterCount(); i++) {
            Class<?> type1 = method1.getParameterTypes()[i];
            Class<?> type2 = method2.getParameterTypes()[i];
            if (!(type1.isAssignableFrom(type2) || type2.isAssignableFrom(type1))) {
                return false;
            }
        }
        return true;
    }

    private <T> boolean hasMediatorAnnotations(AnnotatedMethod<? super T> method) {
        return method.isAnnotationPresent(Incomings.class) || method.isAnnotationPresent(Incoming.class)
                || method.isAnnotationPresent(Outgoing.class);
    }

    private boolean hasMediatorAnnotations(Method m) {
        return m.isAnnotationPresent(Incomings.class) || m.isAnnotationPresent(Incoming.class)
                || m.isAnnotationPresent(Outgoing.class);
    }

    @SuppressWarnings("ConstantConditions")
    public AbstractMediator createMediator(MediatorConfiguration configuration) {
        AbstractMediator mediator = mediatorFactory.create(configuration);
        mediator.setDecorators(decorators);
        mediator.setSubscriberDecorators(subscriberDecorators);
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

    public void start() {
        // Register connectors and other "ends" managed externally.
        registars.stream().forEach(ChannelRegistar::initialize);

        wiring.prepare(strictMode, registry, emitters, channels, collected.mediators());
        Graph graph = wiring.resolve();

        if (graph.hasWiringErrors()) {
            DeploymentException composite = new DeploymentException("Wiring error(s) detected in application.");
            for (Exception error : graph.getWiringErrors()) {
                composite.addSuppressed(error);
            }
            throw composite;
        }

        graph.materialize(registry);

        health.markInitialized();
    }

    // Visible for testing purpose only.
    CollectedMediatorMetadata getCollected() {
        return collected;
    }
}
