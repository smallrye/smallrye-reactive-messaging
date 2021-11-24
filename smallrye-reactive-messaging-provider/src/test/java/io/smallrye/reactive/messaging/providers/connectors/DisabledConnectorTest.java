package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBase;

public class DisabledConnectorTest extends WeldTestBase {

    @BeforeAll
    public static void setupConfig() {
        installConfig("src/test/resources/config/dummy-connector-config-disabled.properties");
    }

    @AfterAll
    public static void clear() {
        releaseConfig();
    }

    @Test
    public void test() {
        initialize();

        assertThat(registry(container).getPublishers("dummy.source")).isEmpty();
        assertThat(registry(container).getSubscribers("dummy-sink")).isEmpty();
    }

}
