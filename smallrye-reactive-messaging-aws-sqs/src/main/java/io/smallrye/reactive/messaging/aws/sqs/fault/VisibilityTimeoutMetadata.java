package io.smallrye.reactive.messaging.aws.sqs.fault;

public class VisibilityTimeoutMetadata {

    private final int visibilityTimeout;

    public VisibilityTimeoutMetadata(int visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public int getVisibilityTimeout() {
        return visibilityTimeout;
    }
}
