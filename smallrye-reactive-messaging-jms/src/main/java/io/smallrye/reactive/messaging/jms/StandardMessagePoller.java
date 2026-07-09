package io.smallrye.reactive.messaging.jms;

import java.util.List;

import jakarta.jms.JMSConsumer;

import org.eclipse.microprofile.reactive.messaging.Message;

class StandardMessagePoller implements JmsMessagePoller {

    private final JmsResourceHolder<JMSConsumer> holder;
    private final long receiveTimeoutMs;
    private final JmsTransactionMode txMode;

    StandardMessagePoller(JmsResourceHolder<JMSConsumer> holder, long receiveTimeoutMs, JmsTransactionMode txMode) {
        this.holder = holder;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.txMode = txMode;
    }

    @Override
    public Message<jakarta.jms.Message> poll() throws Exception {
        jakarta.jms.Message received = receiveTimeoutMs > 0
                ? holder.getClient().receive(receiveTimeoutMs)
                : holder.getClient().receive();
        if (received == null) {
            return null;
        }
        if (txMode != JmsTransactionMode.NONE) {
            return Message.of(received, List.of(new JmsSessionContext(holder.getContext(), txMode)));
        }
        return Message.of(received);
    }

    @Override
    public void close() {
        holder.close();
    }
}
