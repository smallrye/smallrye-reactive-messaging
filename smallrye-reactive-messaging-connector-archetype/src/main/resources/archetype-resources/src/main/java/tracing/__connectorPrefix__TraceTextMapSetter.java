package ${package}.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum ${connectorPrefix}TraceTextMapSetter implements TextMapSetter<${connectorPrefix}Trace> {
    INSTANCE;

    @Override
    public void set(final ${connectorPrefix}Trace carrier, final String key, final String value) {
        if (carrier != null) {
            // fill message properties from key, value attribute
            Map<String, String> properties = carrier.getMessageProperties();
            properties.put(key, value);
        }
    }
}
