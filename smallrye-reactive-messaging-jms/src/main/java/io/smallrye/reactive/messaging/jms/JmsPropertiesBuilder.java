package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;

import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import jakarta.jms.Message;

import io.smallrye.reactive.messaging.jms.impl.JmsTask;

public class JmsPropertiesBuilder {

    private final Map<String, Property<?>> properties = new LinkedHashMap<>();

    public JmsPropertiesBuilder with(String key, boolean value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setBooleanProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder with(String key, byte value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setByteProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder with(String key, short value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setShortProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder with(String key, int value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setIntProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder with(String key, long value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setLongProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder with(String key, float value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setFloatProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder with(String key, double value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setDoubleProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder with(String key, String value) {
        validate(key, value);
        properties.put(key, new Property<>(key,
                value,
                JmsTask.wrap(m -> m.setStringProperty(key, value))));
        return this;
    }

    public JmsPropertiesBuilder without(String key) {
        properties.remove(key);
        return this;
    }

    private void validate(String key, Object value) {
        if (key == null || key.trim().length() == 0) {
            throw ex.illegalStateKeyNull();
        }
        if (value == null) {
            throw ex.illegalStateValueNull();
        }
    }

    public JmsProperties build() {
        return new OutgoingJmsProperties(properties);
    }

    public static class OutgoingJmsProperties implements JmsProperties {

        private final Map<String, Property<?>> properties;

        public OutgoingJmsProperties(Map<String, Property<?>> properties) {
            this.properties = properties;
        }

        @Override
        public boolean propertyExists(String name) {
            return properties.containsKey(name);
        }

        @SuppressWarnings("unchecked")
        private <T> Optional<Property<T>> get(String name) {
            return Optional.ofNullable((Property<T>) properties.get(name));
        }

        @Override
        public boolean getBooleanProperty(String name) {
            Optional<Property<Boolean>> property = get(name);
            return property.map(Property::value).orElse(false);
        }

        @Override
        public byte getByteProperty(String name) {
            Optional<Property<Byte>> property = get(name);
            return property.map(Property::value).orElse((byte) 0);
        }

        @Override
        public short getShortProperty(String name) {
            Optional<Property<Short>> property = get(name);
            return property.map(Property::value).orElse((short) 0);
        }

        @Override
        public int getIntProperty(String name) {
            Optional<Property<Integer>> property = get(name);
            return property.map(Property::value).orElse(0);
        }

        @Override
        public long getLongProperty(String name) {
            Optional<Property<Long>> property = get(name);
            return property.map(Property::value).orElse(0L);
        }

        @Override
        public float getFloatProperty(String name) {
            Optional<Property<Float>> property = get(name);
            return property.map(Property::value).orElse(0.0f);
        }

        @Override
        public double getDoubleProperty(String name) {
            Optional<Property<Double>> property = get(name);
            return property.map(Property::value).orElse(0.0);
        }

        @Override
        public String getStringProperty(String name) {
            Optional<Property<Object>> property = get(name);
            return property.map(p -> p.value().toString()).orElse(null);
        }

        @Override
        public Object getObjectProperty(String name) {
            Optional<Property<Object>> property = get(name);
            return property.map(Property::value).orElse(null);
        }

        @Override
        public Enumeration<String> getPropertyNames() {
            return Collections.enumeration(properties.keySet());
        }

        public Collection<Property<?>> getProperties() {
            return properties.values();
        }
    }

    public static class Property<T> {
        private final String name;
        private final T value;
        private final Consumer<Message> applier;

        private Property(String name, T value, Consumer<Message> applier) {
            this.name = name;
            this.value = value;
            this.applier = applier;
        }

        public T value() {
            return value;
        }

        public String name() {
            return name;
        }

        public void apply(Message message) {
            applier.accept(message);
        }
    }
}
