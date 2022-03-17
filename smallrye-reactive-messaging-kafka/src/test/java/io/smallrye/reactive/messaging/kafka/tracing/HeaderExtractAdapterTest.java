package io.smallrye.reactive.messaging.kafka.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class HeaderExtractAdapterTest {
    @Test
    public void verifyNullHeaderHandled() {
        Headers headers = new RecordHeaders();
        headers.add("test_null_header", null);

        KafkaTrace kafkaTrace = new KafkaTrace.Builder().withHeaders(headers).build();

        String headerValue = KafkaTraceTextMapGetter.INSTANCE.get(kafkaTrace, "test_null_header");

        assertThat(headerValue).isNull();
    }
}
