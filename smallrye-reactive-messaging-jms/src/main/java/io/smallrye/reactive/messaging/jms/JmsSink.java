package io.smallrye.reactive.messaging.jms;

import java.lang.IllegalStateException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.jms.*;
import javax.json.bind.Jsonb;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JmsSink {

    private final JMSProducer producer;
    private final Destination destination;
    private final SubscriberBuilder<Message<?>, Void> sink;
    private final JMSContext context;
    private final Jsonb json;
    private final Executor executor;
    private static final Logger LOGGER = LoggerFactory.getLogger(JmsSink.class);

    JmsSink(JMSContext context, Config config, Jsonb jsonb, Executor executor) {
        String name = config.getOptionalValue("destination", String.class)
                .orElseGet(() -> config.getValue("channel-name", String.class));

        this.destination = getDestination(context, name, config);
        this.context = context;
        this.json = jsonb;
        this.executor = executor;

        producer = context.createProducer();
        config.getOptionalValue("delivery-delay", Long.TYPE).ifPresent(producer::setDeliveryDelay);
        config.getOptionalValue("delivery-mode", String.class).ifPresent(v -> {
            if (v.equalsIgnoreCase("persistent")) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else if (v.equalsIgnoreCase("non_persistent")) {
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            } else {
                throw new IllegalArgumentException(
                        "Invalid delivery mode, it should be either `persistent` or `non_persistent`: " + v);
            }
        });
        config.getOptionalValue("disable-message-id", Boolean.TYPE).ifPresent(producer::setDisableMessageID);
        config.getOptionalValue("disable-message-timestamp", Boolean.TYPE)
                .ifPresent(producer::setDisableMessageTimestamp);
        config.getOptionalValue("correlation-id", String.class).ifPresent(producer::setJMSCorrelationID);
        config.getOptionalValue("ttl", Long.TYPE).ifPresent(producer::setTimeToLive);
        config.getOptionalValue("priority", Integer.TYPE).ifPresent(producer::setPriority);
        config.getOptionalValue("reply-to", String.class).ifPresent(rt -> producer.setJMSReplyTo(
                config.getOptionalValue("reply-to-destination-type", String.class).map(type -> {
                    if (type.equalsIgnoreCase("queue")) {
                        return context.createQueue(rt);
                    } else if (type.equalsIgnoreCase("topic")) {
                        return context.createTopic(rt);
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid reply-to-destination-type, it should be either `queue` (default) or `topic`: " + type);
                    }
                }).orElseGet(() -> context.createQueue(rt))));

        sink = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(m -> {
                    try {
                        return send(m);
                    } catch (JMSException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .onError(t -> LOGGER.error("Unable to send message to JMS", t))
                .ignore();

    }

    private CompletionStage<Message<?>> send(Message<?> message) throws JMSException {
        Object payload = message.getPayload();

        // If the payload is a JMS Message, send it as it is, ignoring headers.
        if (payload instanceof javax.jms.Message) {
            return dispatch(message, () -> producer.send(destination, (javax.jms.Message) payload));
        }

        javax.jms.Message outgoing;
        if (payload instanceof String || payload.getClass().isPrimitive() || isPrimitiveBoxed(payload.getClass())) {
            outgoing = context.createTextMessage(payload.toString());
            outgoing.setStringProperty("_classname", payload.getClass().getName());
            outgoing.setJMSType(payload.getClass().getName());
        } else if (payload.getClass().isArray() && payload.getClass().getComponentType().equals(Byte.TYPE)) {
            BytesMessage o = context.createBytesMessage();
            o.writeBytes((byte[]) payload);
            outgoing = o;
        } else {
            outgoing = context.createTextMessage(json.toJson(payload));
            outgoing.setJMSType(payload.getClass().getName());
            outgoing.setStringProperty("_classname", payload.getClass().getName());
        }

        Headers headers = message.getHeaders();

        String correlationId = headers.getAsString(JmsHeaders.OUTGOING_CORRELATION_ID, null);
        Destination replyTo = headers.get(JmsHeaders.OUTGOING_REPLY_TO, (Destination) null);
        Destination dest = headers.get(JmsHeaders.OUTGOING_DESTINATION, (Destination) null);
        int deliveryMode = headers.getAsInteger(JmsHeaders.OUTGOING_DELIVERY_MODE, -1);
        String type = headers.getAsString(JmsHeaders.OUTGOING_TYPE, null);
        JmsProperties properties = headers.get(JmsHeaders.OUTGOING_PROPERTIES, (JmsProperties) null);

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
                throw new javax.jms.IllegalStateException("Unable to map JMS properties to the outgoing message, "
                        + "OutgoingJmsProperties expected, found " + properties.getClass().getName());
            }
            JmsPropertiesBuilder.OutgoingJmsProperties op = ((JmsPropertiesBuilder.OutgoingJmsProperties) properties);
            op.getProperties().forEach(p -> p.apply(outgoing));
        }

        Destination actualDestination = dest != null ? dest : this.destination;
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

    private Destination getDestination(JMSContext context, String name, Config config) {
        String type = config.getOptionalValue("destination-type", String.class).orElse("queue");
        switch (type.toLowerCase()) {
            case "queue":
                return context.createQueue(name);
            case "topic":
                return context.createTopic(name);
            default:
                throw new IllegalArgumentException("Unknown destination type: " + type);
        }

    }

    SubscriberBuilder<Message<?>, Void> getSink() {
        return sink;
    }

}
