package io.smallrye.reactive.messaging.mqtt.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ReconnectDelayOptionsTest {

    @Nested
    class ConstantReconnectDelayOptionsTests {

        @Test
        void defaultDelayIsTenSeconds() {
            ConstantReconnectDelayOptions options = new ConstantReconnectDelayOptions();
            assertThat(options.getDelay()).isEqualTo(Duration.ofSeconds(10));
        }

        @Test
        void customDelay() {
            ConstantReconnectDelayOptions options = new ConstantReconnectDelayOptions();
            options.setDelay(Duration.ofSeconds(5));
            assertThat(options.getDelay()).isEqualTo(Duration.ofSeconds(5));
        }

        @Test
        void nextDelayReturnsConstantValue() {
            ConstantReconnectDelayOptions options = new ConstantReconnectDelayOptions();
            options.setDelay(Duration.ofSeconds(3));
            ReconnectDelayProvider provider = options.createProvider();

            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(3));
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(3));
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(3));
        }

        @Test
        void resetIsNoOp() {
            ConstantReconnectDelayOptions options = new ConstantReconnectDelayOptions();
            options.setDelay(Duration.ofSeconds(7));
            ReconnectDelayProvider provider = options.createProvider();

            provider.nextDelay();
            provider.reset();
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(7));
        }

        @Test
        void copyProducesIndependentClone() {
            ConstantReconnectDelayOptions original = new ConstantReconnectDelayOptions();
            original.setDelay(Duration.ofSeconds(42));

            ReconnectDelayOptions copy = original.copy();
            assertThat(copy).isInstanceOf(ConstantReconnectDelayOptions.class);

            ConstantReconnectDelayOptions copyTyped = (ConstantReconnectDelayOptions) copy;
            assertThat(copyTyped.getDelay()).isEqualTo(Duration.ofSeconds(42));

            // Modifying original does not affect copy
            original.setDelay(Duration.ofSeconds(99));
            assertThat(copyTyped.getDelay()).isEqualTo(Duration.ofSeconds(42));
        }
    }

    @Nested
    class ExponentialBackoffDelayOptionsTests {

        @Test
        void defaultValues() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            assertThat(options.getMinimum()).isEqualTo(Duration.ofSeconds(1));
            assertThat(options.getIncrement()).isEqualTo(Duration.ofSeconds(1));
            assertThat(options.getMaximum()).isEqualTo(Duration.ofMinutes(5));
        }

        @Test
        void firstDelayIsMinimum() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMinimum(Duration.ofSeconds(2));
            options.setIncrement(Duration.ofSeconds(1));
            options.setMaximum(Duration.ofMinutes(5));

            ReconnectDelayProvider provider = options.createProvider();
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(2));
        }

        @Test
        void delaysGrowExponentially() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMinimum(Duration.ofSeconds(1));
            options.setIncrement(Duration.ofSeconds(1));
            options.setMaximum(Duration.ofMinutes(5));

            ReconnectDelayProvider provider = options.createProvider();
            // count=0: min = 1s
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(1));
            // count=1: min + inc * 2^0 = 1 + 1 = 2s
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(2));
            // count=2: min + inc * 2^1 = 1 + 2 = 3s
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(3));
            // count=3: min + inc * 2^2 = 1 + 4 = 5s
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(5));
            // count=4: min + inc * 2^3 = 1 + 8 = 9s
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(9));
        }

        @Test
        void delayCappedAtMaximum() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMinimum(Duration.ofSeconds(1));
            options.setIncrement(Duration.ofSeconds(1));
            options.setMaximum(Duration.ofSeconds(5));

            ReconnectDelayProvider provider = options.createProvider();
            Duration last = Duration.ZERO;
            for (int i = 0; i < 20; i++) {
                Duration delay = provider.nextDelay();
                assertThat(delay).isLessThanOrEqualTo(Duration.ofSeconds(5));
                last = delay;
            }
            // Eventually should reach the maximum
            assertThat(last).isEqualTo(Duration.ofSeconds(5));
        }

        @Test
        void resetResetsToMinimum() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMinimum(Duration.ofSeconds(1));
            options.setIncrement(Duration.ofSeconds(1));
            options.setMaximum(Duration.ofMinutes(5));

            ReconnectDelayProvider provider = options.createProvider();
            provider.nextDelay(); // 1s
            provider.nextDelay(); // 2s
            provider.nextDelay(); // 3s

            provider.reset();

            // Should restart from minimum
            assertThat(provider.nextDelay()).isEqualTo(Duration.ofSeconds(1));
        }

        @Test
        void negativeIncrementThrows() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setIncrement(Duration.ofSeconds(-1));
            assertThatThrownBy(options::createProvider)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void zeroIncrementThrows() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setIncrement(Duration.ZERO);
            assertThatThrownBy(options::createProvider)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void negativeMaximumThrows() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMaximum(Duration.ofSeconds(-1));
            assertThatThrownBy(options::createProvider)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void zeroMaximumThrows() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMaximum(Duration.ZERO);
            assertThatThrownBy(options::createProvider)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void minimumGreaterThanMaximumThrows() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMinimum(Duration.ofSeconds(10));
            options.setMaximum(Duration.ofSeconds(5));
            assertThatThrownBy(options::createProvider)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void negativeMinimumThrows() {
            ExponentialBackoffDelayOptions options = new ExponentialBackoffDelayOptions();
            options.setMinimum(Duration.ofSeconds(-1));
            assertThatThrownBy(options::createProvider)
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void copyProducesIndependentClone() {
            ExponentialBackoffDelayOptions original = new ExponentialBackoffDelayOptions();
            original.setMinimum(Duration.ofSeconds(2));
            original.setIncrement(Duration.ofSeconds(3));
            original.setMaximum(Duration.ofMinutes(10));

            ReconnectDelayOptions copy = original.copy();
            assertThat(copy).isInstanceOf(ExponentialBackoffDelayOptions.class);

            ExponentialBackoffDelayOptions copyTyped = (ExponentialBackoffDelayOptions) copy;
            assertThat(copyTyped.getMinimum()).isEqualTo(Duration.ofSeconds(2));
            assertThat(copyTyped.getIncrement()).isEqualTo(Duration.ofSeconds(3));
            assertThat(copyTyped.getMaximum()).isEqualTo(Duration.ofMinutes(10));

            // Modifying original does not affect copy
            original.setMinimum(Duration.ofSeconds(99));
            assertThat(copyTyped.getMinimum()).isEqualTo(Duration.ofSeconds(2));
        }
    }
}
