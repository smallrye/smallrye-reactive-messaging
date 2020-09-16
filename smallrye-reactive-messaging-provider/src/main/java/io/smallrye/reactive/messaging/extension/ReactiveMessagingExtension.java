package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;

import java.lang.annotation.Annotation;
import java.util.*;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.*;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;

public class ReactiveMessagingExtension implements Extension {

    private final List<MediatorBean<?>> mediatorBeans = new ArrayList<>();
    private final List<InjectionPoint> streamInjectionPoints = new ArrayList<>();
    private final List<InjectionPoint> emitterInjectionPoints = new ArrayList<>();
    private final List<InjectionPoint> mutinyEmitterInjectionPoints = new ArrayList<>();
    private final List<WorkerPoolBean<?>> workerPoolBeans = new ArrayList<>();

    @Inject
    HealthCenter health;

    <T> void processClassesContainingMediators(@Observes ProcessManagedBean<T> event) {
        AnnotatedType<?> annotatedType = event.getAnnotatedBeanClass();
        if (annotatedType.getMethods()
                .stream()
                .anyMatch(m -> m.isAnnotationPresent(Incomings.class) || m.isAnnotationPresent(Incoming.class)
                        || m.isAnnotationPresent(Outgoing.class))) {
            mediatorBeans.add(new MediatorBean<>(event.getBean(), event.getAnnotatedBeanClass()));
        }
    }

    <T> void processBlockingAnnotation(@Observes @WithAnnotations({ Blocking.class }) ProcessAnnotatedType<T> event) {
        AnnotatedType<?> annotatedType = event.getAnnotatedType();
        workerPoolBeans.add(new WorkerPoolBean<>(annotatedType));
    }

    <T extends Publisher<?>> void processStreamPublisherInjectionPoint(@Observes ProcessInjectionPoint<?, T> pip) {
        Channel stream = ChannelProducer.getChannelQualifier(pip.getInjectionPoint());
        if (stream != null) {
            streamInjectionPoints.add(pip.getInjectionPoint());
        }
    }

    <T extends Emitter<?>> void processStreamEmitterInjectionPoint(@Observes ProcessInjectionPoint<?, T> pip) {
        Channel stream = ChannelProducer.getChannelQualifier(pip.getInjectionPoint());
        if (stream != null) {
            emitterInjectionPoints.add(pip.getInjectionPoint());
        }
    }

    <T extends MutinyEmitter<?>> void processStreamMutinyEmitterInjectionPoint(@Observes ProcessInjectionPoint<?, T> pip) {
        Channel stream = ChannelProducer.getChannelQualifier(pip.getInjectionPoint());
        if (stream != null) {
            mutinyEmitterInjectionPoints.add(pip.getInjectionPoint());
        }
    }

    @SuppressWarnings("deprecation")
    <T extends io.smallrye.reactive.messaging.annotations.Emitter<?>> void processStreamLegacyEmitterInjectionPoint(
            @Observes ProcessInjectionPoint<?, T> pip) {
        Channel stream = ChannelProducer.getChannelQualifier(pip.getInjectionPoint());
        if (stream != null) {
            emitterInjectionPoints.add(pip.getInjectionPoint());
        }
    }

    <T extends PublisherBuilder<?>> void processStreamPublisherBuilderInjectionPoint(
            @Observes ProcessInjectionPoint<?, T> pip) {
        Channel stream = ChannelProducer.getChannelQualifier(pip.getInjectionPoint());
        if (stream != null) {
            streamInjectionPoints.add(pip.getInjectionPoint());
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    void afterDeploymentValidation(@Observes AfterDeploymentValidation done, BeanManager beanManager) {
        Instance<Object> instance = beanManager.createInstance();
        ChannelRegistry registry = instance.select(ChannelRegistry.class)
                .get();

        List<EmitterConfiguration> emitters = new ArrayList<>();
        createEmitterConfiguration(emitterInjectionPoints, false, emitters);
        createEmitterConfiguration(mutinyEmitterInjectionPoints, true, emitters);

        WorkerPoolRegistry workerPoolRegistry = instance.select(WorkerPoolRegistry.class).get();

        for (WorkerPoolBean workerPoolBean : workerPoolBeans) {
            workerPoolRegistry.analyzeWorker(workerPoolBean.annotatedType);
        }

        MediatorManager mediatorManager = instance.select(MediatorManager.class)
                .get();
        mediatorManager.initializeEmitters(emitters);

        for (MediatorBean mediatorBean : mediatorBeans) {
            log.analyzingMediatorBean(mediatorBean.bean);
            mediatorManager.analyze(mediatorBean.annotatedType, mediatorBean.bean);
        }
        mediatorBeans.clear();

        try {
            mediatorManager.initializeAndRun();

            // NOTE: We do not validate @Channel annotations added by portable extensions
            Set<String> names = registry.getIncomingNames();
            for (InjectionPoint ip : streamInjectionPoints) {
                String name = ChannelProducer.getChannelName(ip);
                if (!names.contains(name)) {
                    done.addDeploymentProblem(ex.deploymentNoChannel(name, ip));
                }
                // TODO validate the required type
            }
            streamInjectionPoints.clear();

            for (InjectionPoint ip : emitterInjectionPoints) {
                String name = ChannelProducer.getChannelName(ip);
                EmitterImpl<?> emitter = (EmitterImpl<?>) registry.getEmitter(name);
                if (!emitter.isSubscribed()) {
                    // Subscription may happen later, just print a warning.
                    // Attempting an emission without being subscribed would result in an error.
                    log.noSubscriberForChannelAttachedToEmitter(name, ip.getBean().getBeanClass().getName(),
                            ip.getMember().getName());
                }
                // TODO validate the required type
            }

            for (InjectionPoint ip : mutinyEmitterInjectionPoints) {
                String name = ChannelProducer.getChannelName(ip);
                MutinyEmitterImpl<?> mutinyEmitter = (MutinyEmitterImpl<?>) registry.getMutinyEmitter(name);
                if (!mutinyEmitter.isSubscribed()) {
                    // Subscription may happen later, just print a warning.
                    // Attempting an emission without being subscribed would result in an error.
                    log.noSubscriberForChannelAttachedToEmitter(name, ip.getBean().getBeanClass().getName(),
                            ip.getMember().getName());
                }
                // TODO validate the required type
            }

        } catch (Exception e) {
            done.addDeploymentProblem(e);
            if (health != null) {
                health.report("deployment", e);
            }
        }
    }

    private void createEmitterConfiguration(List<InjectionPoint> emitterInjectionPoints, boolean isMutinyEmitter,
            List<EmitterConfiguration> emitters) {
        for (InjectionPoint point : emitterInjectionPoints) {
            String name = ChannelProducer.getChannelName(point);
            OnOverflow onOverflow = point.getAnnotated().getAnnotation(OnOverflow.class);
            if (onOverflow == null) {
                onOverflow = createOnOverflowForLegacyAnnotation(point);
            }
            Broadcast broadcast = point.getAnnotated().getAnnotation(Broadcast.class);
            emitters.add(new EmitterConfiguration(name, isMutinyEmitter, onOverflow, broadcast));
        }
    }

    @SuppressWarnings("deprecation")
    private OnOverflow createOnOverflowForLegacyAnnotation(InjectionPoint point) {
        io.smallrye.reactive.messaging.annotations.OnOverflow legacy = point.getAnnotated()
                .getAnnotation(io.smallrye.reactive.messaging.annotations.OnOverflow.class);
        if (legacy != null) {
            return new OnOverflow() {

                @Override
                public Class<? extends Annotation> annotationType() {
                    return OnOverflow.class;
                }

                @Override
                public Strategy value() {
                    return Strategy.valueOf(legacy.value().name());
                }

                @Override
                public long bufferSize() {
                    return legacy.bufferSize();
                }
            };
        }
        return null;
    }

    static class MediatorBean<T> {

        final Bean<T> bean;

        final AnnotatedType<T> annotatedType;

        MediatorBean(Bean<T> bean, AnnotatedType<T> annotatedType) {
            this.bean = bean;
            this.annotatedType = annotatedType;
        }

    }

    static class WorkerPoolBean<T> {
        final AnnotatedType<T> annotatedType;

        WorkerPoolBean(AnnotatedType<T> annotatedType) {
            this.annotatedType = annotatedType;
        }
    }
}
