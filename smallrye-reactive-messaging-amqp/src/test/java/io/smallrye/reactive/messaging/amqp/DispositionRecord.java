package io.smallrye.reactive.messaging.amqp;

import org.apache.qpid.proton.amqp.transport.DeliveryState;

class DispositionRecord {
    private int messageNumber;
    private DeliveryState state;
    private boolean settled;

    DispositionRecord(int messageNumber, DeliveryState state, boolean settled) {
        this.messageNumber = messageNumber;
        this.state = state;
        this.settled = settled;
    }

    public int getMessageNumber() {
        return messageNumber;
    }

    public DeliveryState getState() {
        return state;
    }

    public boolean isSettled() {
        return settled;
    }
}