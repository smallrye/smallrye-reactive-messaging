package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.jboss.logmanager.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.blocking.beans.IncomingBlockingExceptionBean;
import io.smallrye.testing.logging.LogCapture;

public class BlockingExceptionTest extends WeldTestBaseWithoutTails {

    @RegisterExtension
    static LogCapture logCapture = LogCapture.with(r -> "io.smallrye.reactive.messaging.provider".equals(r.getLoggerName()),
            Level.ERROR);

    @Test
    public void testIncomingBlockingWithException() {
        addBeanClass(BlockingSubscriberTest.ProduceIn.class);
        addBeanClass(IncomingBlockingExceptionBean.class);
        initialize();

        IncomingBlockingExceptionBean bean = container.getBeanManager().createInstance()
                .select(IncomingBlockingExceptionBean.class).get();

        await().until(() -> bean.list().size() == 5);
        assertThat(bean.list()).contains("a", "b", "d", "e", "f");

        assertThat(logCapture.records()).isNotNull()
                .filteredOn(r -> r.getMessage().contains("SRMSG00200"))
                .hasSize(1)
                .allSatisfy(r -> assertThat(r.getMessage())
                        .contains("io.smallrye.reactive.messaging.blocking.beans.IncomingBlockingExceptionBean#consume"));
    }
}
