package io.smallrye.reactive.messaging.jms;

import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.json.bind.Jsonb;

public class ReceivedJmsMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {
    private final Message delegate;
    private final Executor executor;
    private final Class<T> clazz;
    private final Jsonb json;

    ReceivedJmsMessage(Message message, Executor executor, Jsonb json) {
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
        return (Class<T>) ReceivedJmsMessage.class.getClassLoader().loadClass(cn);
    }

    public String getJMSMessageID() {
        try {
            return delegate.getJMSMessageID();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public long getJMSTimestamp() {
        try {
            return delegate.getJMSTimestamp();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public byte[] getJMSCorrelationIDAsBytes() {
        try {
            return delegate.getJMSCorrelationIDAsBytes();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getJMSCorrelationID() {
        try {
            return delegate.getJMSCorrelationID();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public Destination getJMSReplyTo() {
        try {
            return delegate.getJMSReplyTo();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public Destination getJMSDestination() {
        try {
            return delegate.getJMSDestination();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getJMSDeliveryMode() {
        try {
            return delegate.getJMSDeliveryMode();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean getJMSRedelivered() {
        try {
            return delegate.getJMSRedelivered();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getJMSType() {
        try {
            return delegate.getJMSType();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public long getJMSExpiration() {
        try {
            return delegate.getJMSExpiration();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public long getJMSDeliveryTime() {
        try {
            return delegate.getJMSDeliveryTime();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getJMSPriority() {
        try {
            return delegate.getJMSPriority();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean propertyExists(String name) {
        try {
            return delegate.propertyExists(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean getBooleanProperty(String name) {
        try {
            return delegate.getBooleanProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public byte getByteProperty(String name) {
        try {
            return delegate.getByteProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public short getShortProperty(String name) {
        try {
            return delegate.getShortProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getIntProperty(String name) {
        try {
            return delegate.getIntProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public long getLongProperty(String name) {
        try {
            return delegate.getLongProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public float getFloatProperty(String name) {
        try {
            return delegate.getFloatProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public double getDoubleProperty(String name) {
        try {
            return delegate.getDoubleProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getStringProperty(String name) {
        try {
            return delegate.getStringProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public Object getObjectProperty(String name) {
        try {
            return delegate.getObjectProperty(name);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public Enumeration getPropertyNames() {
        try {
            return delegate.getPropertyNames();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public T getBody(Class<T> c) {
        try {
            return delegate.getBody(c);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean isBodyAssignableTo(Class c) {
        try {
            return delegate.isBodyAssignableTo(c);
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
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
}
