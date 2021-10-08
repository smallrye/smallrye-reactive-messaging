package io.smallrye.reactive.messaging.metrics;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.smallrye.reactive.messaging.PublisherDecorator;

@ApplicationScoped
public class PublisherDecoratorProducer {
    static final Class<?> metricsClass;
    static {
        Class<?> clazz = null;
        try {
            // If the Micrometer Metrics class is present, use the MicrometerDecorator directly.
            clazz = Class.forName("io.micrometer.core.instrument.Metrics");
        } catch (ClassNotFoundException e) {
        }
        metricsClass = clazz;
    }

    @Produces
    PublisherDecorator createPublisherDecorator() {
        if ( metricsClass == null ) {
            return new MetricDecorator();
        } else {
            return new MicrometerDecorator();
        }
    }
}
