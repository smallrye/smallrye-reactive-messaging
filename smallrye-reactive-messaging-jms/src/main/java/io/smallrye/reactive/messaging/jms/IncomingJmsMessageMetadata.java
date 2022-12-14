package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.impl.Wrap.wrap;

import java.util.Enumeration;

import jakarta.jms.Destination;
import jakarta.jms.Message;

import io.smallrye.reactive.messaging.jms.impl.ImmutableJmsProperties;

public class IncomingJmsMessageMetadata implements JmsMessageMetadata, JmsProperties {

    private final Message message;
    private final ImmutableJmsProperties properties;

    public IncomingJmsMessageMetadata(Message incoming) {
        this.message = incoming;
        this.properties = new ImmutableJmsProperties(incoming);
    }

    public String getMessageId() {
        return wrap(message::getJMSMessageID);
    }

    public long getTimestamp() {
        return wrap(message::getJMSTimestamp);
    }

    @Override
    public String getCorrelationId() {
        return wrap(message::getJMSCorrelationID);
    }

    @Override
    public Destination getReplyTo() {
        return wrap(message::getJMSReplyTo);
    }

    @Override
    public Destination getDestination() {
        return wrap(message::getJMSDestination);
    }

    @Override
    public int getDeliveryMode() {
        return wrap(message::getJMSDeliveryMode);
    }

    public boolean isRedelivered() {
        return wrap(message::getJMSRedelivered);
    }

    @Override
    public String getType() {
        return wrap(message::getJMSType);
    }

    @Override
    public JmsProperties getProperties() {
        return properties;
    }

    public long getExpiration() {
        return wrap(message::getJMSExpiration);
    }

    public long getDeliveryTime() {
        return wrap(message::getJMSDeliveryTime);
    }

    public int getPriority() {
        return wrap(message::getJMSPriority);
    }

    @Override
    public boolean propertyExists(String name) {
        return properties.propertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return properties.getBooleanProperty(name);
    }

    @Override
    public byte getByteProperty(String name) {
        return properties.getByteProperty(name);
    }

    @Override
    public short getShortProperty(String name) {
        return properties.getShortProperty(name);
    }

    @Override
    public int getIntProperty(String name) {
        return properties.getIntProperty(name);
    }

    @Override
    public long getLongProperty(String name) {
        return properties.getLongProperty(name);
    }

    @Override
    public float getFloatProperty(String name) {
        return properties.getFloatProperty(name);
    }

    @Override
    public double getDoubleProperty(String name) {
        return properties.getDoubleProperty(name);
    }

    @Override
    public String getStringProperty(String name) {
        return properties.getStringProperty(name);
    }

    @Override
    public Object getObjectProperty(String name) {
        return properties.getObjectProperty(name);
    }

    @Override
    public Enumeration<String> getPropertyNames() {
        return properties.getPropertyNames();
    }

    public <X> X getBody(Class<X> c) {
        return wrap(() -> message.getBody(c));
    }

    public boolean isBodyAssignableTo(Class<?> c) {
        return wrap(() -> message.isBodyAssignableTo(c));
    }
}
