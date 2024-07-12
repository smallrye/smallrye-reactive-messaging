package connectors.tracing;

import java.util.Collections;
import java.util.List;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public class MyAttributesExtractor implements AttributesExtractor<MyTrace, Void> {
    private final MessagingAttributesGetter<MyTrace, Void> messagingAttributesGetter;

    public MyAttributesExtractor() {
        this.messagingAttributesGetter = new MyMessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final MyTrace myTrace) {
        // fill in attributes from myTrace object
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final MyTrace myTrace,
            final Void unused,
            final Throwable error) {

    }

    public MessagingAttributesGetter<MyTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class MyMessagingAttributesGetter implements MessagingAttributesGetter<MyTrace, Void> {
        @Override
        public String getSystem(final MyTrace myTrace) {
            return "my";
        }

        @Override
        public String getDestination(final MyTrace myTrace) {
            return myTrace.getTopic();
        }

        @Override
        public boolean isTemporaryDestination(final MyTrace myTrace) {
            return false;
        }

        @Override
        public String getConversationId(final MyTrace myTrace) {
            return null;
        }

        @Override
        public Long getMessagePayloadSize(final MyTrace myTrace) {
            return null;
        }

        @Override
        public Long getMessagePayloadCompressedSize(final MyTrace myTrace) {
            return null;
        }

        @Override
        public String getMessageId(final MyTrace myTrace, final Void unused) {
            return null;
        }

        @Override
        public List<String> getMessageHeader(MyTrace myTrace, String name) {
            return Collections.emptyList();
        }

        @Override
        public String getDestinationTemplate(MyTrace myTrace) {
            return null;
        }

        @Override
        public boolean isAnonymousDestination(MyTrace myTrace) {
            return false;
        }

        @Override
        public Long getMessageBodySize(MyTrace myTrace) {
            return null;
        }

        @Override
        public Long getMessageEnvelopeSize(MyTrace myTrace) {
            return null;
        }

        @Override
        public String getClientId(MyTrace myTrace) {
            return null;
        }

        @Override
        public Long getBatchMessageCount(MyTrace myTrace, Void unused) {
            return null;
        }

    }
}
