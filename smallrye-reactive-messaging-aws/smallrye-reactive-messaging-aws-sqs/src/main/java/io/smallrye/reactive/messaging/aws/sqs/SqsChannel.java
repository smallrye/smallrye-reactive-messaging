package io.smallrye.reactive.messaging.aws.sqs;

/**
 * @author Christopher Holomek
 * @since 01.10.2023
 */
public abstract class SqsChannel {

    protected boolean closed = false;

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
    }
}
