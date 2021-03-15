package io.smallrye.reactive.messaging.broadcast;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class BroadcastTest extends WeldTestBaseWithoutTails {

    @Test
    public void testBroadcast() {
        addBeanClass(BeanUsingBroadcast.class);
        initialize();

        BeanUsingBroadcast bean = container.getBeanManager().createInstance().select(BeanUsingBroadcast.class).get();

        await().until(() -> bean.l1().size() == 4);
        await().until(() -> bean.l2().size() == 4);

        assertThat(bean.l1()).containsExactly("A", "B", "C", "D").containsExactlyElementsOf(bean.l2());
    }

    @Test
    public void testBroadcastOfEmitter() {
        addBeanClass(BeanEmitterBroadcast.class, BeanEmitterConsumer.class);
        initialize();

        BeanEmitterBroadcast broadcastAndConsumer = get(BeanEmitterBroadcast.class);
        BeanEmitterConsumer consumer = get(BeanEmitterConsumer.class);

        broadcastAndConsumer.send("a");
        broadcastAndConsumer.send("b");
        broadcastAndConsumer.send("c");
        broadcastAndConsumer.send("d");

        assertThat(broadcastAndConsumer.emitter()).isNotNull();

        await().until(() -> broadcastAndConsumer.list().size() == 4);
        await().until(() -> consumer.list().size() == 4);

        assertThat(broadcastAndConsumer.list()).containsExactly("a", "b", "c", "d").containsExactlyElementsOf(consumer.list());
    }

    @Test
    public void testBroadcastOfMutinyEmitter() {
        addBeanClass(BeanMutinyEmitterBroadcast.class, BeanMutinyEmitterConsumer.class);
        initialize();

        BeanMutinyEmitterBroadcast broadcastAndConsumer = get(BeanMutinyEmitterBroadcast.class);
        BeanMutinyEmitterConsumer consumer = get(BeanMutinyEmitterConsumer.class);

        broadcastAndConsumer.send("a");
        broadcastAndConsumer.send("b");
        broadcastAndConsumer.send("c");
        broadcastAndConsumer.send("d");

        assertThat(broadcastAndConsumer.emitter()).isNotNull();

        await().until(() -> broadcastAndConsumer.list().size() == 4);
        await().until(() -> consumer.list().size() == 4);

        assertThat(broadcastAndConsumer.list()).containsExactly("a", "b", "c", "d").containsExactlyElementsOf(consumer.list());
    }
}
