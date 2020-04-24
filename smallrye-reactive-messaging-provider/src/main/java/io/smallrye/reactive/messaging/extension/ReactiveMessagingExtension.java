package io.smallrye.reactive.messaging.extension;

import java.lang.annotation.Annotation;
import java.util.*;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;

import org.eclipse.microprofile.reactive.messaging.*;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;

public class ReactiveMessagingExtension implements Extension {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMessagingExtension.class);

    private List<MediatorBean<?>> mediatorBeans = new ArrayList<>();
    private List<InjectionPoint> streamInjectionPoints = new ArrayList<>();
    private List<InjectionPoint> emitterInjectionPoints = new ArrayList<>();
    private List<WorkerPoolBean<?>> workerPoolBeans = new ArrayList<>();

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

        Map<String, OnOverflow> emitters = new HashMap<>();
        for (InjectionPoint point : emitterInjectionPoints) {
            String name = ChannelProducer.getChannelName(point);
            OnOverflow onOverflow = point.getAnnotated().getAnnotation(OnOverflow.class);
            if (onOverflow == null) {
                onOverflow = createOnOverflowForLegacyAnnotation(point);
            }
            emitters.put(name, onOverflow);
        }

        WorkerPoolRegistry workerPoolRegistry = instance.select(WorkerPoolRegistry.class).get();

        for (WorkerPoolBean workerPoolBean : workerPoolBeans) {
            workerPoolRegistry.analyzeWorker(workerPoolBean.annotatedType);
        }

        MediatorManager mediatorManager = instance.select(MediatorManager.class)
                .get();
        mediatorManager.initializeEmitters(emitters);

        for (MediatorBean mediatorBean : mediatorBeans) {
            LOGGER.info("Analyzing mediator bean: {}", mediatorBean.bean);
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
                    done.addDeploymentProblem(
                            new DeploymentException("No channel found for name: " + name + ", injection point: " + ip));
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
                    LOGGER.warn("No subscriber for channel {}  attached to the emitter {}.{}", name,
                            ip.getBean().getBeanClass().getName(), ip.getMember().getName());
                }
                // TODO validate the required type
            }

        } catch (Exception e) {
            done.addDeploymentProblem(e);
        }
    }

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
