package ${package}.tracing;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;

public class ${connectorPrefix}AttributesExtractor implements AttributesExtractor<${connectorPrefix}Trace, Void> {
    private final MessagingAttributesGetter<${connectorPrefix}Trace, Void> messagingAttributesGetter;

    public ${connectorPrefix}AttributesExtractor() {
        this.messagingAttributesGetter = new ${connectorPrefix}MessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final ${connectorPrefix}Trace myTrace) {
        // fill in attributes from myTrace object
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final ${connectorPrefix}Trace myTrace,
            final Void unused,
            final Throwable error) {

    }

    public MessagingAttributesGetter<${connectorPrefix}Trace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class ${connectorPrefix}MessagingAttributesGetter implements MessagingAttributesGetter<${connectorPrefix}Trace, Void> {
        @Override
        public String getSystem(final ${connectorPrefix}Trace myTrace) {
            return "my";
        }

        @Override
        public String getDestination(final ${connectorPrefix}Trace myTrace) {
            return myTrace.getTopic();
        }

        @Override
        public boolean isTemporaryDestination(final ${connectorPrefix}Trace myTrace) {
            return false;
        }

        @Override
        public String getConversationId(final ${connectorPrefix}Trace myTrace) {
            return null;
        }

        @Override
        public Long getMessagePayloadSize(final ${connectorPrefix}Trace myTrace) {
            return null;
        }

        @Override
        public Long getMessagePayloadCompressedSize(final ${connectorPrefix}Trace myTrace) {
            return null;
        }

        @Override
        public String getMessageId(final ${connectorPrefix}Trace myTrace, final Void unused) {
            return null;
        }
    }
}
