package io.smallrye.reactive.messaging.jms.tracing;

import static io.opentelemetry.semconv.SemanticAttributes.*;

import java.util.Collections;
import java.util.List;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public class JmsAttributesExtractor implements AttributesExtractor<JmsTrace, Void> {
    private final MessagingAttributesGetter<JmsTrace, Void> messagingAttributesGetter;

    public JmsAttributesExtractor() {
        this.messagingAttributesGetter = new JmsMessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final JmsTrace jmsTrace) {
        String groupId = jmsTrace.getGroupId();
        String clientId = jmsTrace.getClientId();
        if (groupId != null && clientId != null) {
            String consumerId = groupId;
            if (!clientId.isEmpty()) {
                consumerId += " - " + clientId;
            }
            attributes.put(MESSAGING_CONSUMER_ID, consumerId);
        }
        if (groupId != null) {
            attributes.put(MESSAGING_KAFKA_CONSUMER_GROUP, groupId);
        }
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final JmsTrace kafkaTrace,
            final Void unused,
            final Throwable error) {

    }

    public MessagingAttributesGetter<JmsTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class JmsMessagingAttributesGetter implements MessagingAttributesGetter<JmsTrace, Void> {
        @Override
        public String getSystem(final JmsTrace jmsTrace) {
            return "jms";
        }

        @Override
        public String getDestination(final JmsTrace jmsTrace) {
            return jmsTrace.getQueue();
        }

        @Override
        public boolean isTemporaryDestination(final JmsTrace jmsTrace) {
            return false;
        }

        @Override
        public String getConversationId(final JmsTrace jmsTrace) {
            return null;
        }

        @Override
        public String getMessageId(final JmsTrace jmsTrace, final Void unused) {
            return null;
        }

        @Override
        public List<String> getMessageHeader(JmsTrace jmsTrace, String name) {
            return Collections.emptyList();
        }

        @Override
        public String getDestinationTemplate(JmsTrace jmsTrace) {
            return null;
        }

        @Override
        public boolean isAnonymousDestination(JmsTrace jmsTrace) {
            return false;
        }

        @Override
        public Long getMessageBodySize(JmsTrace jmsTrace) {
            return null;
        }

        @Override
        public Long getMessageEnvelopeSize(JmsTrace jmsTrace) {
            return null;
        }

        @Override
        public String getClientId(JmsTrace jmsTrace) {
            if (jmsTrace.getClientId() == null) {
                return null;
            }
            return jmsTrace.getClientId();
        }

        @Override
        public Long getBatchMessageCount(JmsTrace jmsTrace, Void unused) {
            return null;
        }
    }
}
