package io.smallrye.reactive.messaging.connectors;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBase;

public class DisabledConnectorTest extends WeldTestBase {

    @BeforeClass
    public static void setupConfig() {
        installConfig("src/test/resources/config/dummy-connector-config-disabled.properties");
    }

    @AfterClass
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
