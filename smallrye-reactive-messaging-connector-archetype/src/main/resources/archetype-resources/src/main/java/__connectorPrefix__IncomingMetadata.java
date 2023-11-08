package ${package};

import java.util.Map;

import ${package}.api.ConsumedMessage;

public class ${connectorPrefix}IncomingMetadata<T> {

    private final ConsumedMessage<T> msg;

    public ${connectorPrefix}IncomingMetadata(ConsumedMessage<T> msg) {
        this.msg = msg;
    }

    public ConsumedMessage<T> getCustomMessage() {
        return msg;
    }

    public T getBody() {
        return msg.body();
    }

    public String getKey() {
        return msg.key();
    }

    public long getTimestamp() {
        return msg.timestamp();
    }

    public Map<String, String> getProperties() {
        return msg.properties();
    }
}
