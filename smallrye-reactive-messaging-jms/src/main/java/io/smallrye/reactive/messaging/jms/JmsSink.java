package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.time.Duration;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;

import jakarta.enterprise.inject.Instance;
import jakarta.jms.BytesMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.jms.tracing.JmsOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.jms.tracing.JmsTrace;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;

class JmsSink {

    private final Flow.Subscriber<Message<?>> sink;
    private final JsonMapping jsonMapping;
    private final Executor executor;
    private final JmsOpenTelemetryInstrumenter jmsInstrumenter;
    private final boolean isTracingEnabled;

    JmsSink(JmsResourceHolder<JMSProducer> resourceHolder, JmsConnectorOutgoingConfiguration config,
            Instance<OpenTelemetry> openTelemetryInstance, JsonMapping jsonMapping,
            Executor executor) {
        this.isTracingEnabled = config.getTracingEnabled();

        String name = config.getDestination().orElseGet(config::getChannel);
        String type = config.getDestinationType();
        boolean retry = config.getRetry();
        int retryMaxRetries = config.getRetryMaxRetries();
        Duration retryInitialDelay = Duration.parse(config.getRetryInitialDelay());
        Duration retryMaxDelay = Duration.parse(config.getRetryMaxDelay());
        double retryJitter = config.getRetryJitter();
        resourceHolder.configure(r -> getDestination(r.getContext(), name, type),
                r -> {
                    JMSContext context = r.getContext();
                    JMSProducer producer = context.createProducer();
                    config.getDeliveryDelay().ifPresent(producer::setDeliveryDelay);
                    config.getDeliveryMode().ifPresent(v -> {
                        if (v.equalsIgnoreCase("persistent")) {
                            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                        } else if (v.equalsIgnoreCase("non_persistent")) {
                            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                        } else {
                            throw ex.illegalArgumentInvalidDeliveryMode(v);
                        }
                    });
                    config.getDisableMessageId().ifPresent(producer::setDisableMessageID);
                    config.getDisableMessageTimestamp().ifPresent(producer::setDisableMessageTimestamp);
                    config.getCorrelationId().ifPresent(producer::setJMSCorrelationID);
                    config.getTtl().ifPresent(producer::setTimeToLive);
                    config.getPriority().ifPresent(producer::setPriority);
                    config.getReplyTo().ifPresent(rt -> {
                        String replyToDestinationType = config.getReplyToDestinationType();
                        Destination replyToDestination;
                        if (replyToDestinationType.equalsIgnoreCase("topic")) {
                            replyToDestination = context.createTopic(rt);
                        } else if (replyToDestinationType.equalsIgnoreCase("queue")) {
                            replyToDestination = context.createQueue(rt);
                        } else {
                            throw ex.illegalArgumentInvalidDestinationType(replyToDestinationType);
                        }
                        producer.setJMSReplyTo(replyToDestination);
                    });
                    return producer;
                });
        resourceHolder.getDestination();
        resourceHolder.getClient();
        this.jsonMapping = jsonMapping;
        this.executor = executor;

        if (isTracingEnabled) {
            jmsInstrumenter = JmsOpenTelemetryInstrumenter.createForSink(openTelemetryInstance);
        } else {
            jmsInstrumenter = null;
        }

        sink = MultiUtils.via(m -> m.onItem().transformToUniAndConcatenate(message -> send(resourceHolder, message)
                .onFailure(t -> retry)
                .retry()
                .withJitter(retryJitter)
                .withBackOff(retryInitialDelay, retryMaxDelay)
                .atMost(retryMaxRetries))
                .onFailure().invoke(log::unableToSend));

    }

    private Uni<? extends Message<?>> send(JmsResourceHolder<JMSProducer> resourceHolder, Message<?> message) {
        Object payload = message.getPayload();

        Destination destination = resourceHolder.getDestination();
        JMSContext context = resourceHolder.getContext();
        // If the payload is a JMS Message, send it as it is, ignoring metadata.
        if (payload instanceof jakarta.jms.Message) {
            outgoingTrace(destination, message, (jakarta.jms.Message) payload);
            return dispatch(message,
                    () -> resourceHolder.getClient().send(destination, (jakarta.jms.Message) payload));
        }

        try {
            jakarta.jms.Message outgoing;
            if (payload instanceof String || payload.getClass().isPrimitive() || isPrimitiveBoxed(payload.getClass())) {
                outgoing = context.createTextMessage(payload.toString());
                outgoing.setStringProperty("_classname", payload.getClass().getName());
                outgoing.setJMSType(payload.getClass().getName());
            } else if (payload.getClass().isArray() && payload.getClass().getComponentType().equals(Byte.TYPE)) {
                BytesMessage o = context.createBytesMessage();
                o.writeBytes((byte[]) payload);
                outgoing = o;
            } else {
                outgoing = context.createTextMessage(jsonMapping.toJson(payload));
                outgoing.setJMSType(payload.getClass().getName());
                outgoing.setStringProperty("_classname", payload.getClass().getName());
            }

            OutgoingJmsMessageMetadata metadata = message.getMetadata(OutgoingJmsMessageMetadata.class).orElse(null);
            Destination actualDestination;
            if (metadata != null) {
                String correlationId = metadata.getCorrelationId();
                Destination replyTo = metadata.getReplyTo();
                Destination dest = metadata.getDestination();
                int deliveryMode = metadata.getDeliveryMode();
                String type = metadata.getType();
                JmsProperties properties = metadata.getProperties();
                if (correlationId != null) {
                    outgoing.setJMSCorrelationID(correlationId);
                }
                if (replyTo != null) {
                    outgoing.setJMSReplyTo(replyTo);
                }
                if (dest != null) {
                    outgoing.setJMSDestination(dest);
                }
                if (deliveryMode != -1) {
                    outgoing.setJMSDeliveryMode(deliveryMode);
                }
                if (type != null) {
                    outgoing.setJMSType(type);
                }
                if (type != null) {
                    outgoing.setJMSType(type);
                }

                if (properties != null) {
                    if (!(properties instanceof JmsPropertiesBuilder.OutgoingJmsProperties)) {
                        throw ex.illegalStateUnableToMapProperties(properties.getClass().getName());
                    }
                    JmsPropertiesBuilder.OutgoingJmsProperties op = ((JmsPropertiesBuilder.OutgoingJmsProperties) properties);
                    op.getProperties().forEach(p -> p.apply(outgoing));
                }
                actualDestination = dest != null ? dest : destination;
            } else {
                actualDestination = destination;
            }

            outgoingTrace(actualDestination, message, outgoing);
            return dispatch(message, () -> resourceHolder.getClient().send(actualDestination, outgoing));
        } catch (JMSException e) {
            return Uni.createFrom().failure(new IllegalStateException(e));
        }
    }

    private void outgoingTrace(Destination actualDestination, Message<?> message, jakarta.jms.Message payload) {
        if (isTracingEnabled) {
            jakarta.jms.Message jmsPayload = payload;
            Map<String, Object> messageProperties = new HashMap<>();
            try {
                Enumeration<?> propertyNames = jmsPayload.getPropertyNames();

                while (propertyNames.hasMoreElements()) {
                    String propertyName = (String) propertyNames.nextElement();
                    messageProperties.put(propertyName, jmsPayload.getObjectProperty(propertyName));
                }
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
            JmsTrace jmsTrace = new JmsTrace.Builder()
                    .withQueue(actualDestination.toString())
                    .withProperties(messageProperties)
                    .build();
            jmsInstrumenter.traceOutgoing(message, jmsTrace);
        }
    }

    private boolean isPrimitiveBoxed(Class<?> c) {
        return c.equals(Boolean.class)
                || c.equals(Integer.class)
                || c.equals(Byte.class)
                || c.equals(Double.class)
                || c.equals(Float.class)
                || c.equals(Short.class)
                || c.equals(Character.class)
                || c.equals(Long.class);
    }

    private Uni<? extends Message<?>> dispatch(Message<?> incoming, Runnable action) {
        return Uni.createFrom().item(incoming)
                .invoke(action)
                .call(message -> Uni.createFrom().completionStage(incoming::ack))
                .runSubscriptionOn(executor);
    }

    private Destination getDestination(JMSContext context, String name, String type) {
        switch (type.toLowerCase()) {
            case "queue":
                return context.createQueue(name);
            case "topic":
                return context.createTopic(name);
            default:
                throw ex.illegalStateUnknownDestinationType(type);
        }

    }

    Flow.Subscriber<Message<?>> getSink() {
        return sink;
    }

}
