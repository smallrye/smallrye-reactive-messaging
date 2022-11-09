package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.lang.IllegalStateException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import jakarta.jms.*;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.json.JsonMapping;

class JmsSink {

    private final JMSProducer producer;
    private final Destination destination;
    private final SubscriberBuilder<Message<?>, Void> sink;
    private final JMSContext context;
    private final JsonMapping jsonMapping;
    private final Executor executor;

    JmsSink(JMSContext context, JmsConnectorOutgoingConfiguration config, JsonMapping jsonMapping, Executor executor) {
        String name = config.getDestination().orElseGet(config::getChannel);

        this.destination = getDestination(context, name, config.getDestinationType());
        this.context = context;
        this.jsonMapping = jsonMapping;
        this.executor = executor;

        producer = context.createProducer();
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

        sink = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(m -> {
                    try {
                        return send(m);
                    } catch (JMSException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .onError(t -> log.unableToSend(t))
                .ignore();

    }

    private CompletionStage<Message<?>> send(Message<?> message) throws JMSException {
        Object payload = message.getPayload();

        // If the payload is a JMS Message, send it as it is, ignoring metadata.
        if (payload instanceof jakarta.jms.Message) {
            return dispatch(message, () -> producer.send(destination, (jakarta.jms.Message) payload));
        }

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
            actualDestination = dest != null ? dest : this.destination;
        } else {
            actualDestination = this.destination;
        }

        return dispatch(message, () -> producer.send(actualDestination, outgoing));
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

    private CompletionStage<Message<?>> dispatch(Message<?> incoming, Runnable action) {
        return CompletableFuture.runAsync(action, executor)
                .thenCompose(x -> incoming.ack())
                .thenApply(x -> incoming);
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

    SubscriberBuilder<Message<?>, Void> getSink() {
        return sink;
    }

}
