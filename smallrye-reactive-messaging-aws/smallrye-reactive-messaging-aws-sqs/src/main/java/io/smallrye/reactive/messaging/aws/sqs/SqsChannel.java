package io.smallrye.reactive.messaging.aws.sqs;

public abstract class SqsChannel {

    protected boolean closed = false;

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
    }
}
