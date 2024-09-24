package io.smallrye.reactive.messaging.kafka.reply;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply.DEFAULT_REPLY_CORRELATION_ID_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordConverter;

public class KafkaRequestReplyTest extends KafkaCompanionTestBase {

    private KafkaMapBasedConfig config() {
        return kafkaConfig()
                .withPrefix("mp.messaging.connector.smallrye-kafka")
                .with("graceful-shutdown", false)
                .with("client-id-prefix", topic + "-")
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("topic", topic)
                .with("poll-timeout", 100)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("value.deserializer", StringDeserializer.class.getName())
                .withPrefix("mp.messaging.incoming.req")
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("fail-on-deserialization-failure", "false")
                .withPrefix("mp.messaging.outgoing.rep")
                .with("propagate-record-key", true)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", StringSerializer.class.getName());
    }

    @Test
    void testReply() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<String> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config(), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(i).subscribe().with(replies::add);
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyWithConverter() {
        addBeans(ReplyServer.class, ConsumerRecordConverter.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducerWithConverter app = runApplication(config(), RequestReplyProducerWithConverter.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(i).subscribe().with(replies::add);
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value)
                .containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyMessage() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config(), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyWithReplyTopic() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-rep";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config().withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.topic", replyTopic), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyWithSameTopic() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config().withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.topic", topic)
                .with("reply.fail-on-deserialization-failure", "false"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }

        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyWithReplyPartition() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config().withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.partition", 2), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .allSatisfy(record -> assertThat(record.partition()).isEqualTo(2))
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyWithConsumerConfig() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        String replyTopicConsumer = topic + "-consumer";
        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config().withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.group.id", replyTopicConsumer)
                .with("batch", true)
                .with("commit-strategy", "latest"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(companion.consumerGroups().list()).extracting(ConsumerGroupListing::groupId)
                .contains(replyTopicConsumer);
        await().untilAsserted(() -> assertThat(companion.consumerGroups().offsets(replyTopicConsumer)).isNotEmpty());
    }

    @Test
    void testReplyWithCustomHeadersReplyServerMessage() {
        addBeans(ReplyServerMessageWithCustomHeaders.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config().withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.partition", 2)
                .with("reply.correlation-id.header", "MY_CORRELATION")
                .with("reply.topic.header", "MY_TOPIC")
                .with("reply.partition.header", "MY_PARTITION")
                .withPrefix("mp.messaging.outgoing.rep")
                .with("propagate-headers", "MY_CORRELATION"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .allSatisfy(record -> assertThat(record.partition()).isEqualTo(2))
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyWithCustomHeaders() {
        addBeans(ReplyServerWithCustomHeaders.class, ConsumerRecordConverter.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config().withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.partition", 2)
                .with("reply.correlation-id.header", "MY_CORRELATION")
                .with("reply.topic.header", "MY_TOPIC")
                .with("reply.partition.header", "MY_PARTITION"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .allSatisfy(record -> assertThat(record.partition()).isEqualTo(2))
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyMultipleEmittersSameTopic() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducerSecond app = runApplication(config().withPrefix("mp.messaging.outgoing.request-reply2")
                .with("topic", topic)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("value.deserializer", StringDeserializer.class.getName()), RequestReplyProducerSecond.class);

        for (int i = 0; i < 20; i++) {
            KafkaRequestReply<Integer, String> requestReply = (i % 2 == 0) ? app.requestReply() : app.requestReply2();
            requestReply.request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }

        await().untilAsserted(() -> assertThat(replies).hasSize(20));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 20).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19");

        assertThat(app.requestReply().getPendingReplies()).isEmpty();
        assertThat(app.requestReply2().getPendingReplies()).isEmpty();
    }

    @Test
    void testReplyMultipleEmittersDifferentPartitions() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducerSecond app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.partition", 0)
                .withPrefix("mp.messaging.outgoing.request-reply2")
                .with("topic", topic)
                .with("reply.partition", 1)
                .with("key.serializer", StringSerializer.class.getName())
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("value.deserializer", StringDeserializer.class.getName()), RequestReplyProducerSecond.class);

        for (int i = 0; i < 20; i++) {
            KafkaRequestReply<Integer, String> requestReply = (i % 2 == 0) ? app.requestReply() : app.requestReply2();
            requestReply.request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }

        await().untilAsserted(() -> assertThat(replies).hasSize(20));
        assertThat(replies).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 20).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19");

        assertThat(app.requestReply().getPendingReplies()).isEmpty();
        assertThat(app.requestReply2().getPendingReplies()).isEmpty();
    }

    @Test
    void testReplyMessageBytesCorrelationId() {
        addBeans(ReplyServer.class, BytesCorrelationIdHandler.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.correlation-id.handler", "bytes"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies)
                .allSatisfy(r -> {
                    byte[] value = r.headers().lastHeader(DEFAULT_REPLY_CORRELATION_ID_HEADER).value();
                    String base64 = new BytesCorrelationId(value).toString();
                    assertThat(value)
                            .hasSize(12)
                            .isEqualTo(Base64.getDecoder().decode(base64))
                            .asBase64Encoded().isEqualTo(base64);
                })
                .extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyFailureHandler() {
        addBeans(ReplyServerWithFailure.class, MyReplyFailureHandler.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.failure.handler", "my-reply-error"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i))
                    .subscribe().with(m -> {
                        IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                        replies.add(metadata.getRecord());
                    }, errors::add);
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(6));
        assertThat(replies)
                .extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("1", "2", "4", "5", "7", "8");
        await().untilAsserted(() -> assertThat(errors).hasSize(4));
        assertThat(errors)
                .extracting(Throwable::getMessage)
                .allSatisfy(message -> assertThat(message).containsAnyOf("0", "3", "6", "9")
                        .contains("Cannot reply to"));
    }

    @Test
    void testReplyOffsetResetEarliest() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<String> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.auto.offset.reset", "earliest"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(Message.of(i, Metadata.of(OutgoingKafkaRecordMetadata.builder()
                    .withKey("" + i).build()))).subscribe().with(r -> replies.add(r.getPayload()));
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyWithoutWaitForAssignment() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        List<String> replies = new CopyOnWriteArrayList<>();

        RequestReplyProducer app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.initial-assignment-timeout", "-1"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(Message.of(i, Metadata.of(OutgoingKafkaRecordMetadata.builder()
                    .withKey("" + i).build()))).subscribe().with(r -> replies.add(r.getPayload()));
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromTopics(replyTopic, 10).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyAssignAndSeekOffset() {
        addBeans(ReplyServer.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = companion.topics().createAndWait(topic + "-replies", 3);

        List<ConsumerRecord<String, String>> replies = new CopyOnWriteArrayList<>();
        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(replyTopic, 2, "k" + i, String.valueOf(i)), 10)
                .awaitCompletion();

        RequestReplyProducer app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.partition", 2)
                .with("reply.assign-seek", "2:10"), RequestReplyProducer.class);

        for (int i = 0; i < 10; i++) {
            app.requestReply().request(KafkaRecord.of(String.valueOf(i), i)).subscribe().with(m -> {
                IncomingKafkaRecordMetadata metadata = m.getMetadata(IncomingKafkaRecordMetadata.class).get();
                replies.add(metadata.getRecord());
            });
        }
        await().untilAsserted(() -> assertThat(replies).hasSize(10));
        assertThat(replies)
                .allSatisfy(record -> assertThat(record.partition()).isEqualTo(2))
                .extracting(ConsumerRecord::offset)
                .containsExactlyInAnyOrder(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L);

        assertThat(replies)
                .extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

        assertThat(companion.consumeStrings().fromOffsets(Map.of(tp(replyTopic, 2), 10L), 10).awaitCompletion())
                .extracting(ConsumerRecord::value).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testReplyTimeout() {
        addBeans(ReplyServerSlow.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        RequestReplyProducer app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("reply.timeout", "1000"), RequestReplyProducer.class);

        app.requestReply().request(1)
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitFailure().assertFailedWith(TimeoutException.class);
    }

    @Test
    void testReplyGracefulShutdown() {
        addBeans(ReplyServerSlow.class);
        topic = companion.topics().createAndWait(topic, 3);
        String replyTopic = topic + "-replies";
        companion.topics().createAndWait(replyTopic, 3);

        RequestReplyProducer app = runApplication(config()
                .withPrefix("mp.messaging.outgoing.request-reply")
                .with("graceful-shutdown", true), RequestReplyProducer.class);

        app.requestReply().request(1).subscribe().withSubscriber(UniAssertSubscriber.create());
        app.requestReply().request(2).subscribe().withSubscriber(UniAssertSubscriber.create());
        await().untilAsserted(() -> assertThat(app.requestReply().getPendingReplies()).hasSize(2));
        assertThat(app.requestReply().getPendingReplies().values())
                .extracting(PendingReply::replyTopic).containsOnly(replyTopic);

        await().untilAsserted(() -> assertThat(app.requestReply().getPendingReplies()).isEmpty());
    }

    @ApplicationScoped
    public static class RequestReplyProducer {

        @Inject
        @Channel("request-reply")
        KafkaRequestReply<Integer, String> requestReply;

        public KafkaRequestReply<Integer, String> requestReply() {
            return requestReply;
        }

    }

    @ApplicationScoped
    public static class RequestReplyProducerWithConverter {

        @Inject
        @Channel("request-reply")
        KafkaRequestReply<Integer, ConsumerRecord<String, String>> requestReply;

        public KafkaRequestReply<Integer, ConsumerRecord<String, String>> requestReply() {
            return requestReply;
        }

    }

    @ApplicationScoped
    public static class RequestReplyProducerSecond {

        @Inject
        @Channel("request-reply")
        KafkaRequestReply<Integer, String> requestReply;

        @Inject
        @Channel("request-reply2")
        KafkaRequestReply<Integer, String> requestReply2;

        public KafkaRequestReply<Integer, String> requestReply() {
            return requestReply;
        }

        public KafkaRequestReply<Integer, String> requestReply2() {
            return requestReply2;
        }

    }

    @ApplicationScoped
    public static class ReplyServer {

        @Incoming("req")
        @Outgoing("rep")
        String process(Integer payload) {
            if (payload == null) {
                return null;
            }
            return String.valueOf(payload);
        }
    }

    @ApplicationScoped
    public static class ReplyServerSlow {

        @Incoming("req")
        @Outgoing("rep")
        @Blocking
        String process(int payload) throws InterruptedException {
            Thread.sleep(3000);
            return String.valueOf(payload);
        }
    }

    @ApplicationScoped
    public static class ReplyServerWithFailure {

        @Incoming("req")
        @Outgoing("rep")
        Message<String> process(Message<Integer> msg) {
            OutgoingKafkaRecord<Object, String> out = KafkaRecord.from(msg).withPayload(null);
            if (msg.getPayload() % 3 == 0) {
                return out
                        .addMetadata(OutgoingKafkaRecordMetadata.builder()
                                .addHeaders(new RecordHeader("REPLY_ERROR", ("Cannot reply to " + msg.getPayload()).getBytes()))
                                .build());
            }
            return out.withPayload(String.valueOf(msg.getPayload()));
        }
    }

    @ApplicationScoped
    @Identifier("my-reply-error")
    public static class MyReplyFailureHandler implements ReplyFailureHandler {

        @Override
        public Throwable handleReply(KafkaRecord<?, ?> replyRecord) {
            Header header = replyRecord.getHeaders().lastHeader("REPLY_ERROR");
            if (header != null) {
                return new IllegalArgumentException(new String(header.value()));
            }
            return null;
        }
    }

    @ApplicationScoped
    public static class ReplyServerMessageWithCustomHeaders {

        @Incoming("req")
        @Outgoing("rep")
        Message<String> process(Message<Integer> msg) {
            IncomingKafkaRecordMetadata<String, Integer> im = msg.getMetadata(IncomingKafkaRecordMetadata.class).get();
            String myTopic = new String(im.getHeaders().lastHeader("MY_TOPIC").value());
            int myPartition = KafkaRequestReply.replyPartitionFromBytes(im.getHeaders().lastHeader("MY_PARTITION").value());
            return KafkaRecord.from(msg)
                    .withPayload(String.valueOf(msg.getPayload()))
                    .addMetadata(OutgoingKafkaRecordMetadata.builder()
                            .withTopic(myTopic)
                            .withPartition(myPartition)
                            .build());
        }
    }

    @ApplicationScoped
    public static class ReplyServerWithCustomHeaders {

        @Incoming("req")
        @Outgoing("rep")
        ProducerRecord<String, String> process(ConsumerRecord<String, Integer> record) {
            String myTopic = new String(record.headers().lastHeader("MY_TOPIC").value());
            int myPartition = KafkaRequestReply.replyPartitionFromBytes(record.headers().lastHeader("MY_PARTITION").value());
            Headers headers = new RecordHeaders(record.headers()).remove("MY_TOPIC");
            return new ProducerRecord<>(myTopic, myPartition, record.key(), String.valueOf(record.value()), headers);
        }
    }

}
