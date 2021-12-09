package io.smallrye.reactive.messaging.kafka.companion.test;

import java.lang.reflect.Method;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

/**
 * Test base starting Kafka broker using {@link KafkaBrokerExtension} and providing {@link KafkaCompanion}.
 */
@ExtendWith(KafkaBrokerExtension.class)
public class KafkaCompanionTestBase {

    public static KafkaCompanion companion;

    public String topic;

    @BeforeAll
    static void initCompanion(@KafkaBrokerExtension.KafkaBootstrapServers String bootstrapServers) {
        companion = new KafkaCompanion(bootstrapServers);
    }

    @BeforeEach
    public void initTopic(TestInfo testInfo) {
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        topic = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterAll
    static void closeCompanion() {
        companion.close();
    }

}
