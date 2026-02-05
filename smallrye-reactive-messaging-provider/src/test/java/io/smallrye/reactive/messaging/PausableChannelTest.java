package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;

public class PausableChannelTest extends WeldTestBaseWithoutTails {

    @Test
    public void testPausableChannelInitiallyPausedLegacy() {
        releaseConfig();
        installConfig("src/test/resources/config/pausable.properties");
        addBeanClass(ConsumerApp.class);

        initialize();

        ConsumerApp app = get(ConsumerApp.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("B");

        await().pollDelay(3, TimeUnit.SECONDS).until(() -> app.getCount() == 0);
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(1L));

        pauser.pause();
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(2L));

        pauser.pause();
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(3L));

        assertThat(app.get()).containsExactly(2, 3, 4);

    }

    @Test
    public void testPausableChannelInitiallyPaused() {
        releaseConfig();
        installConfig("src/test/resources/config/pausable-initially-paused.properties");
        addBeanClass(ConsumerApp.class);

        initialize();

        ConsumerApp app = get(ConsumerApp.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("B");

        await().pollDelay(3, TimeUnit.SECONDS).until(() -> app.getCount() == 0);
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(1L));

        pauser.pause();
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(2L));

        pauser.pause();
        assertThat(pauser.isPaused()).isTrue();
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(3L));

        assertThat(app.get()).containsExactly(2, 3, 4);

    }

    @Test
    public void testPausableChannelWithBufferSize() {
        releaseConfig();
        installConfig("src/test/resources/config/pausable-buffer-size.properties");
        addBeanClass(ConsumerApp.class);

        initialize();

        ConsumerApp app = get(ConsumerApp.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("B");

        // Channel starts unpaused, wait for some messages
        await().untilAsserted(() -> assertThat(app.getCount()).isGreaterThanOrEqualTo(1L));

        // Pause and verify it's paused
        pauser.pause();
        assertThat(pauser.isPaused()).isTrue();

        // Resume and verify all messages are delivered (dummy connector emits 3 messages: 2, 3, 4)
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(3L));

        assertThat(app.get()).containsExactly(2, 3, 4);
    }

    @Test
    public void testPausableChannelWithBufferStrategyDrop() {
        releaseConfig();
        installConfig("src/test/resources/config/pausable-buffer-drop.properties");
        addBeanClass(ConsumerApp.class);

        initialize();

        ConsumerApp app = get(ConsumerApp.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("B");

        // Channel starts unpaused, wait for first message
        await().untilAsserted(() -> assertThat(app.getCount()).isGreaterThanOrEqualTo(1L));

        // Pause the channel
        pauser.pause();
        assertThat(pauser.isPaused()).isTrue();

        // Resume and verify all messages complete (dummy connector emits 3 messages: 2, 3, 4)
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(3L));

        assertThat(app.get()).isNotEmpty();
    }

    @Test
    public void testPausableChannelWithLateSubscription() {
        releaseConfig();
        installConfig("src/test/resources/config/pausable-late-subscription.properties");
        addBeanClass(ConsumerApp.class);

        initialize();

        ConsumerApp app = get(ConsumerApp.class);
        ChannelRegistry pausableChannels = get(ChannelRegistry.class);
        PausableChannel pauser = pausableChannels.getPausable("B");

        // With late subscription and initially paused, no messages should flow initially
        await().pollDelay(3, TimeUnit.SECONDS).until(() -> app.getCount() == 0);
        assertThat(pauser.isPaused()).isTrue();

        // Resume and verify messages start flowing (dummy connector emits 3 messages: 2, 3, 4)
        pauser.resume();
        await().untilAsserted(() -> assertThat(app.getCount()).isGreaterThanOrEqualTo(1L));

        // Eventually all 3 messages should be delivered
        await().untilAsserted(() -> assertThat(app.getCount()).isEqualTo(3L));
        assertThat(app.get()).containsExactly(2, 3, 4);
    }

    @ApplicationScoped
    public static class ConsumerApp {

        LongAdder count = new LongAdder();
        List<Integer> list = new CopyOnWriteArrayList<>();

        @Incoming("B")
        @Blocking
        public void consume(Integer message) {
            list.add(message);
            count.increment();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public List<Integer> get() {
            return list;
        }

        public long getCount() {
            return count.longValue();
        }
    }

}
