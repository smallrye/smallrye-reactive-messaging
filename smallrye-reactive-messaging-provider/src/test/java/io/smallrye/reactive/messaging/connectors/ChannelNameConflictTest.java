package io.smallrye.reactive.messaging.connectors;

import javax.enterprise.inject.spi.DeploymentException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBase;

public class ChannelNameConflictTest extends WeldTestBase {

    @BeforeClass
    public static void setupConfig() {
        installConfig("src/test/resources/config/channel-name-conflict.properties");
    }

    @AfterClass
    public static void clear() {
        releaseConfig();
    }

    @Test(expected = DeploymentException.class)
    public void test() {
        initializer.addBeanClasses(DummyBean.class);
        initialize();
    }
}
