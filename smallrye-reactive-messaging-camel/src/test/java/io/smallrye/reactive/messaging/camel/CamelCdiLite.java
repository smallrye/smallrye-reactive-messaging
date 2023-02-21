package io.smallrye.reactive.messaging.camel;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.reactive.streams.ReactiveStreamsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.engine.DefaultComponentNameResolver;
import org.apache.camel.support.DefaultRegistry;

public class CamelCdiLite {

    @Inject
    Instance<RouteBuilder> builders;

    @Produces
    @ApplicationScoped
    public DefaultCamelContext get() {
        DefaultCamelContext context = new DefaultCamelContext();
        context.disableJMX();
        context.setApplicationContextClassLoader(CamelConnector.class.getClassLoader());
        context.setRegistry(new DefaultRegistry());
        context.setLoadTypeConverters(false);
        context.build();
        context.setComponentNameResolver(new DefaultComponentNameResolver());

        context.addComponent("reactive-streams", new ReactiveStreamsComponent());

        builders.stream().forEach(rb -> {
            try {
                context.addRoutes(rb);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        context.start();

        return context;

    }

}
