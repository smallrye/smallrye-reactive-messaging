package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.impl.VertxBeanRegistration.registerVertxBeanIfNeeded;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.LogManager;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessManagedBean;
import javax.enterprise.inject.spi.WithAnnotations;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.annotations.Stream;

public class ReactiveMessagingExtension implements Extension {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMessagingExtension.class);

    private PublisherInjectionManager injections = new PublisherInjectionManager();

    private List<MediatorBean<?>> mediatorBeans = new ArrayList<>();

    <T> void processClassesContainingMediators(@Observes ProcessManagedBean<T> event) {
        AnnotatedType<?> annotatedType = event.getAnnotatedBeanClass();
        if (annotatedType.getMethods()
                .stream()
                .anyMatch(m -> m.isAnnotationPresent(Incoming.class) || m.isAnnotationPresent(Outgoing.class))) {
            mediatorBeans.add(new MediatorBean<>(event.getBean(), event.getAnnotatedBeanClass()));
        }
    }

    <T> void processClassesRequestingPublisher(@Observes @WithAnnotations({ Stream.class }) ProcessAnnotatedType<T> pat) {
        injections.analyze(pat);
    }

    /**
     * In this callback, regular beans have been found, we can declare new beans.
     *
     * @param discovery the discovery event
     * @param beanManager the bean manager
     */
    void afterBeanDiscovery(@Observes AfterBeanDiscovery discovery, BeanManager beanManager) {
        registerVertxBeanIfNeeded(discovery, beanManager);
        LOGGER.info("Creating synthetic beans for injection points");
        injections.createBeans(discovery);

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    void afterDeploymentValidation(@Observes AfterDeploymentValidation done, BeanManager beanManager) {
        Instance<Object> instance = beanManager.createInstance();
        StreamRegistry registry = instance.select(StreamRegistry.class)
                .get();
        injections.setRegistry(registry);

        MediatorManager mediatorManager = instance.select(MediatorManager.class)
                .get();
        for (MediatorBean mediatorBean : mediatorBeans) {
            LOGGER.info("Analyzing mediator bean:" + mediatorBean.bean);
            mediatorManager.analyze(mediatorBean.annotatedType, mediatorBean.bean);
        }
        mediatorBeans.clear();
        CompletableFuture<Void> future = mediatorManager.initializeAndRun();
        try {
            future.get();
        } catch (ExecutionException e) {
            done.addDeploymentProblem(e.getCause());
        } catch (InterruptedException e) {
            done.addDeploymentProblem(e);
        }
    }

    static class MediatorBean<T> {

        final Bean<T> bean;

        final AnnotatedType<T> annotatedType;

        MediatorBean(Bean<T> bean, AnnotatedType<T> annotatedType) {
            this.bean = bean;
            this.annotatedType = annotatedType;
        }

    }

}
