package io.smallrye.reactive.messaging;

public class DefauiltKeyMetadata {

    private final Object messageKey;

    public DefauiltKeyMetadata(Object messageKey) {
        this.messageKey = messageKey;
    }

    public Object getMessageKey() {
        return messageKey;
    }
}
