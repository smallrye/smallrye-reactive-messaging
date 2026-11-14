package io.smallrye.reactive.messaging.mqtt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;

/**
 * Two channels connected to the same broker (same host, port, user and client id) share a single
 * {@link Clients.ClientHolder}, and so a single MQTT connection. Each channel has its own message buffer, sized by its
 * own {@code buffer-size}, so each channel must have its own pause and resume thresholds.
 */
public class ClientHolderThresholdTest {

    private final AtomicInteger pauseCount = new AtomicInteger();
    private final AtomicInteger resumeCount = new AtomicInteger();
    private final AtomicBoolean paused = new AtomicBoolean();

    private Clients.ClientHolder holderOfAConnectedClient() {
        MqttClientSession session = mock(MqttClientSession.class);
        when(session.isConnected()).thenReturn(true);
        when(session.isPaused()).thenAnswer(ignored -> paused.get());
        doAnswer(ignored -> {
            paused.set(true);
            pauseCount.incrementAndGet();
            return null;
        }).when(session).pause();
        doAnswer(ignored -> {
            paused.set(false);
            resumeCount.incrementAndGet();
            return null;
        }).when(session).resume();
        return new Clients.ClientHolder(session);
    }

    /**
     * The channel with the smallest buffer is registered first, so the thresholds of the second channel (700 / 400)
     * are the ones in use. Its own buffer is never going to reach 700 messages, so the reading is never paused and the
     * buffer overflows.
     */
    @Test
    public void testChannelWithTheSmallestBufferIsPaused() {
        Clients.ClientHolder holder = holderOfAConnectedClient();
        holder.registerChannelBuffer("small-buffer", 100, 70, 40);
        holder.registerChannelBuffer("large-buffer", 1000, 70, 40);

        // Fill the whole buffer of the first channel: it must be paused after 70 messages.
        for (int i = 0; i < 100; i++) {
            holder.messageEnterBuffer("small-buffer");
        }

        assertThat(pauseCount.get()).isEqualTo(1);
    }

    /**
     * The same, the other way around: the channel with the largest buffer is registered first, so the thresholds of
     * the second channel (70 / 40) are the ones in use, and the reading is paused when the first channel has only used
     * 7% of its buffer.
     */
    @Test
    public void testChannelWithTheLargestBufferIsNotPausedTooEarly() {
        Clients.ClientHolder holder = holderOfAConnectedClient();
        holder.registerChannelBuffer("large-buffer", 1000, 70, 40);
        holder.registerChannelBuffer("small-buffer", 100, 70, 40);

        // 71 messages out of a buffer of 1000: the pause threshold of this channel is 700.
        for (int i = 0; i < 71; i++) {
            holder.messageEnterBuffer("large-buffer");
        }

        assertThat(pauseCount.get()).isZero();
    }

    /**
     * A channel that is released must not be taken into account any more: its last known buffer usage would otherwise
     * block the resume of the channels still running on the same client.
     */
    @Test
    public void testReleasedChannelDoesNotBlockTheResume() {
        Clients.ClientHolder holder = holderOfAConnectedClient();
        holder.retain("stopped").retain("running");
        holder.registerChannelBuffer("stopped", 100, 70, 40);
        holder.registerChannelBuffer("running", 100, 70, 40);

        // The first channel fills its buffer, pausing the reading, and is then released.
        for (int i = 0; i < 71; i++) {
            holder.messageEnterBuffer("stopped");
        }
        assertThat(pauseCount.get()).isEqualTo(1);
        holder.release("stopped");

        // The channel still running consumes its messages: the reading must be resumed.
        holder.messageEnterBuffer("running");
        holder.messageExitBuffer("running");

        assertThat(resumeCount.get()).isEqualTo(1);
    }

    /**
     * A channel cancelled while the reading is paused because of it must not leave it paused: the other channels can
     * only consume what is already in their buffer, so there may be no message left to trigger the resume.
     */
    @Test
    public void testForgettingTheChannelThatPausedResumesTheReading() {
        Clients.ClientHolder holder = holderOfAConnectedClient();
        holder.registerChannelBuffer("cancelled", 100, 70, 40);
        holder.registerChannelBuffer("running", 100, 70, 40);

        for (int i = 0; i < 71; i++) {
            holder.messageEnterBuffer("cancelled");
        }
        assertThat(pauseCount.get()).isEqualTo(1);

        holder.forgetChannelBuffer("cancelled");

        assertThat(resumeCount.get()).isEqualTo(1);
    }

    /**
     * A message can still reach the buffer bookkeeping of a channel that has just been forgotten.
     */
    @Test
    public void testMessagesOfAForgottenChannelAreIgnored() {
        Clients.ClientHolder holder = holderOfAConnectedClient();
        holder.registerChannelBuffer("cancelled", 100, 70, 40);
        holder.forgetChannelBuffer("cancelled");

        for (int i = 0; i < 200; i++) {
            holder.messageEnterBuffer("cancelled");
            holder.messageExitBuffer("cancelled");
        }

        assertThat(pauseCount.get()).isZero();
    }

    /**
     * Once paused because of the channel with the largest buffer, the reading must be resumed when that channel goes
     * below its own resume threshold (400), no matter the thresholds of the other channels of the same client.
     */
    @Test
    public void testReadingIsResumedOnTheThresholdOfTheChannelThatPaused() {
        Clients.ClientHolder holder = holderOfAConnectedClient();
        holder.registerChannelBuffer("large-buffer", 1000, 70, 40);
        holder.registerChannelBuffer("small-buffer", 100, 70, 40);

        for (int i = 0; i < 701; i++) {
            holder.messageEnterBuffer("large-buffer");
        }
        assertThat(pauseCount.get()).isEqualTo(1);

        // Consume down to 400 messages, the resume threshold of this channel.
        for (int i = 0; i < 301; i++) {
            holder.messageExitBuffer("large-buffer");
        }

        assertThat(resumeCount.get()).isEqualTo(1);
    }
}
