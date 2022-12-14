package io.smallrye.reactive.messaging.providers.impl;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.*;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;

import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions;

@ApplicationScoped
public class ConnectorFactories {

    private final Map<String, InboundConnector> inbound;
    private final Map<String, OutboundConnector> outbound;

    // CDI requirement for normal scoped beans
    protected ConnectorFactories() {
        this.inbound = null;
        this.outbound = null;
    }

    @Inject
    public ConnectorFactories(
            @Any Instance<InboundConnector> inc,
            @Any Instance<IncomingConnectorFactory> incFactories,
            @Any Instance<OutboundConnector> out,
            @Any Instance<OutgoingConnectorFactory> outFactories,
            BeanManager beanManager) {
        inbound = new HashMap<>();
        outbound = new HashMap<>();

        beanManager.getBeans(InboundConnector.class, Any.Literal.INSTANCE)
                .forEach(bean -> {
                    String connector = extractConnector(bean);
                    InboundConnector instance = inc.select(ConnectorLiteral.of(connector)).get();
                    addToMap(inbound, connector, instance);
                });

        beanManager.getBeans(IncomingConnectorFactory.class, Any.Literal.INSTANCE)
                .forEach(bean -> {
                    String connector = extractConnector(bean);
                    InboundConnector instance = wrap(incFactories.select(ConnectorLiteral.of(connector)).get());
                    addToMap(inbound, connector, instance);
                });

        beanManager.getBeans(OutboundConnector.class, Any.Literal.INSTANCE)
                .forEach(bean -> {
                    String connector = extractConnector(bean);
                    OutboundConnector instance = out.select(ConnectorLiteral.of(connector)).get();
                    addToMap(outbound, connector, instance);
                });

        beanManager.getBeans(OutgoingConnectorFactory.class, Any.Literal.INSTANCE)
                .forEach(bean -> {
                    String connector = extractConnector(bean);
                    OutboundConnector instance = wrap(outFactories.select(ConnectorLiteral.of(connector)).get());
                    addToMap(outbound, connector, instance);
                });
    }

    private <T> void addToMap(Map<String, T> map, String key, T instance) {
        T old = map.put(key, instance);
        if (old != null) {
            throw ProviderExceptions.ex.multipleBeanDeclaration(key, old.getClass().getName(), instance.getClass().getName());
        }
    }

    private InboundConnector wrap(IncomingConnectorFactory cf) {
        return config -> cf.getPublisherBuilder(config).buildRs();
    }

    private OutboundConnector wrap(OutgoingConnectorFactory cf) {
        return config -> cf.getSubscriberBuilder(config).build();
    }

    /**
     * Extracts the connector qualifier on the given bean and return the {@code value}.
     * If the bean does not have a connector qualifier, throws a {@link DefinitionException}.
     *
     * @param bean the bean
     * @return the connector value
     */
    private String extractConnector(Bean<?> bean) {
        return bean.getQualifiers().stream()
                .filter(a -> a.annotationType().equals(Connector.class))
                .map(annotation -> ((Connector) annotation).value())
                .findAny().orElseThrow(() -> ProviderExceptions.ex.missingConnectorQualifier(bean.getBeanClass().getName()));
    }

    public Map<String, InboundConnector> getInboundConnectors() {
        return inbound;
    }

    public Map<String, OutboundConnector> getOutboundConnectors() {
        return outbound;
    }

}
