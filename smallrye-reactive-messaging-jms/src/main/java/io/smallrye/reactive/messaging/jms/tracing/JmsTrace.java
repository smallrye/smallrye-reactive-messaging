package io.smallrye.reactive.messaging.jms.tracing;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

public class JmsTrace {
    private final String queue;
    private final Message jmsMessage;

    private JmsTrace(final String queue, Message jmsMessage) {
        this.queue = queue;
        this.jmsMessage = jmsMessage;
    }

    public String getQueue() {
        return queue;
    }

    public Message getMessage() {
        return jmsMessage;
    }

    public List<String> getPropertyNames() {
        List<String> keys = new ArrayList<>();
        Enumeration propertyNames = null;
        try {
            propertyNames = jmsMessage.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                keys.add(propertyNames.nextElement().toString());
            }
        } catch (JMSException ignored) {
        }
        return keys;
    }

    public String getProperty(final String key) {
        try {
            return jmsMessage.getStringProperty(key);
        } catch (JMSException ignored) {
            return null;
        }
    }

    public void setProperty(final String key, final String value) {
        try {
            jmsMessage.setStringProperty(key, value);
        } catch (JMSException ignored) {
        }
    }

    public static class Builder {
        private String queue;
        private Message jmsMessage;

        public Builder withQueue(final String queue) {
            this.queue = queue;
            return this;
        }

        public Builder withMessage(final Message message) {
            this.jmsMessage = message;
            return this;
        }

        public JmsTrace build() {
            return new JmsTrace(queue, jmsMessage);
        }
    }
}
