package keyed;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

// <code>
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;

@ApplicationScoped
public class KeyValueExtractorFromPayload implements KeyValueExtractor {

    @Override
    public boolean canExtract(Message<?> msg, Type keyType, Type valueType) {
        // Called for the first message of the stream to select the extractor.
        // Here we only check for the type, but the logic can be more complex
        return keyType.equals(String.class) && valueType.equals(String.class);
    }

    @Override
    public String extractKey(Message<?> message, Type keyType) {
        String string = message.getPayload().toString();
        return string.substring(0, string.indexOf(":"));
    }

    @Override
    public String extractValue(Message<?> message, Type valueType) {
        String string = message.getPayload().toString();
        return string.substring(string.indexOf(":") + 1);
    }

}
// </code>
