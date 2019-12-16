package io.smallrye.reactive.messaging.jms;

import java.util.Enumeration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.json.bind.Jsonb;

public class IncomingJmsMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T>, JmsProperties {
    private final Message delegate;
    private final Executor executor;
    private final Class<T> clazz;
    private final Jsonb json;
    private final JmsProperties properties;

    IncomingJmsMessage(Message message, Executor executor, Jsonb json) {
        this.delegate = message;
        this.json = json;
        this.executor = executor;
        String cn = null;
        try {
            cn = message.getStringProperty("_classname");
        } catch (JMSException e) {
            // ignore it
        }
        try {
            this.clazz = cn != null ? load(cn) : null;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to load the class " + e);
        }
        this.properties = new ImmutableJmsProperties(message);
    }

    @SuppressWarnings("unchecked")
    private Class<T> load(String cn) throws ClassNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader != null) {
            try {
                return (Class<T>) loader.loadClass(cn);
            } catch (ClassNotFoundException e) {
                // Will try with the current class classloader
            }
        }
        return (Class<T>) IncomingJmsMessage.class.getClassLoader().loadClass(cn);
    }

    private <R> R wrap(Callable<R> code) {
        try {
            return code.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String getJMSMessageID() {
        return wrap(delegate::getJMSMessageID);
    }

    public long getJMSTimestamp() {
        return wrap(delegate::getJMSTimestamp);
    }

    public byte[] getJMSCorrelationIDAsBytes() {
        return wrap(delegate::getJMSCorrelationIDAsBytes);
    }

    public String getJMSCorrelationID() {
        return wrap(delegate::getJMSCorrelationID);
    }

    public Destination getJMSReplyTo() {
        return wrap(delegate::getJMSReplyTo);
    }

    public Destination getJMSDestination() {
        return wrap(delegate::getJMSDestination);
    }

    public int getJMSDeliveryMode() {
        return wrap(delegate::getJMSDeliveryMode);
    }

    public boolean getJMSRedelivered() {
        return wrap(delegate::getJMSRedelivered);
    }

    public String getJMSType() {
        return wrap(delegate::getJMSType);
    }

    public long getJMSExpiration() {
        return wrap(delegate::getJMSExpiration);
    }

    public long getJMSDeliveryTime() {
        return wrap(delegate::getJMSDeliveryTime);
    }

    public int getJMSPriority() {
        return wrap(delegate::getJMSPriority);
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
    public Enumeration getPropertyNames() {
        return properties.getPropertyNames();
    }

    public <X> X getBody(Class<X> c) {
        return wrap(() -> delegate.getBody(c));
    }

    public boolean isBodyAssignableTo(Class c) {
        return wrap(() -> delegate.isBodyAssignableTo(c));
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getPayload() {
        try {
            if (clazz != null) {
                return convert(delegate.getBody(String.class));
            } else {
                return (T) delegate.getBody(Object.class);
            }
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private T convert(String value) {
        if (clazz.equals(Integer.class)) {
            return (T) Integer.valueOf(value);
        }
        if (clazz.equals(Long.class)) {
            return (T) Long.valueOf(value);
        }
        if (clazz.equals(Double.class)) {
            return (T) Double.valueOf(value);
        }
        if (clazz.equals(Float.class)) {
            return (T) Float.valueOf(value);
        }
        if (clazz.equals(Boolean.class)) {
            return (T) Boolean.valueOf(value);
        }
        if (clazz.equals(Short.class)) {
            return (T) Short.valueOf(value);
        }
        if (clazz.equals(Byte.class)) {
            return (T) Byte.valueOf(value);
        }

        return json.fromJson(value, clazz);

    }

    @Override
    public CompletionStage<Void> ack() {
        return CompletableFuture.runAsync(() -> {
            try {
                delegate.acknowledge();
            } catch (JMSException e) {
                throw new IllegalArgumentException();
            }
        }, executor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C> C unwrap(Class<C> unwrapType) {
        if (Message.class.equals(unwrapType)) {
            return (C) delegate;
        }
        throw new IllegalArgumentException("Unable to unwrap message to " + unwrapType);
    }

    private final class ImmutableJmsProperties implements JmsProperties {
        private final Message delegate;

        ImmutableJmsProperties(Message message) {
            this.delegate = message;
        }

        @Override
        public boolean propertyExists(String name) {
            return wrap(() -> delegate.propertyExists(name));
        }

        @Override
        public boolean getBooleanProperty(String name) {
            return wrap(() -> delegate.getBooleanProperty(name));
        }

        @Override
        public byte getByteProperty(String name) {
            return wrap(() -> delegate.getByteProperty(name));
        }

        @Override
        public short getShortProperty(String name) {
            return wrap(() -> delegate.getShortProperty(name));
        }

        @Override
        public int getIntProperty(String name) {
            return wrap(() -> delegate.getIntProperty(name));
        }

        @Override
        public long getLongProperty(String name) {
            return wrap(() -> delegate.getLongProperty(name));
        }

        @Override
        public float getFloatProperty(String name) {
            return wrap(() -> delegate.getFloatProperty(name));
        }

        @Override
        public double getDoubleProperty(String name) {
            return wrap(() -> delegate.getDoubleProperty(name));
        }

        @Override
        public String getStringProperty(String name) {
            return wrap(() -> delegate.getStringProperty(name));
        }

        @Override
        public Object getObjectProperty(String name) {
            return wrap(() -> delegate.getObjectProperty(name));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Enumeration<String> getPropertyNames() {
            return wrap(delegate::getPropertyNames);
        }
    }
}
