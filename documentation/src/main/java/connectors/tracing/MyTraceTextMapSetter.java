package connectors.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum MyTraceTextMapSetter implements TextMapSetter<MyTrace> {
    INSTANCE;

    @Override
    public void set(final MyTrace carrier, final String key, final String value) {
        if (carrier != null) {
            // fill message properties from key, value attribute
            Map<String, String> properties = carrier.getMessageProperties();
            properties.put(key, value);
        }
    }
}
