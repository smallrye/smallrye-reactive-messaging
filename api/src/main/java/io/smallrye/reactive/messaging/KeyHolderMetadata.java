package io.smallrye.reactive.messaging;

public class KeyHolderMetadata {

    private final Object messageKey;

    public KeyHolderMetadata(Object messageKey) {
        this.messageKey = messageKey;
    }

    public Object getMessageKey() {
        return messageKey;
    }
}
