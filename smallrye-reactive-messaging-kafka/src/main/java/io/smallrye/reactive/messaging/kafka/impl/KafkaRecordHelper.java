package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply;

public class KafkaRecordHelper {

    public static final String SMALLRYE_GENERATION_ID = "__smallrye_generationId";

    public static Headers getHeaders(OutgoingKafkaRecordMetadata<?> om,
            IncomingKafkaRecordMetadata<?, ?> im,
            RuntimeKafkaSinkConfiguration configuration) {
        Headers headers = new RecordHeaders();
        // First incoming headers, so that they can be overridden by outgoing headers
        if (im != null && im.getHeaders() != null) {
            // propagate reply correlation id
            Header correlationHeader = im.getHeaders().lastHeader(KafkaRequestReply.DEFAULT_REPLY_CORRELATION_ID_HEADER);
            if (correlationHeader != null) {
                headers.add(correlationHeader);
            }
            if (isNotBlank(configuration.getPropagateHeaders())) {
                Set<String> headersToPropagate = Arrays.stream(configuration.getPropagateHeaders().split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());

                for (Header header : im.getHeaders()) {
                    if (headersToPropagate.contains(header.key())) {
                        headers.add(header);
                    }
                }
            }
        }
        // add outgoing metadata headers, and override incoming headers if needed
        if (om != null && om.getHeaders() != null) {
            om.getHeaders().forEach(headers::add);
        }
        return headers;
    }

    public static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    public static void addGenerationIdToHeaders(ConsumerRecord<?, ?> record, ConsumerGroupMetadata metadata) {
        record.headers().add(SMALLRYE_GENERATION_ID, Integer.toString(metadata.generationId()).getBytes());
    }

    public static int extractGenerationIdFrom(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(SMALLRYE_GENERATION_ID);
        if (header != null) {
            record.headers().remove(SMALLRYE_GENERATION_ID);
            return Integer.parseInt(new String(header.value()));
        }
        return -1;
    }

}
