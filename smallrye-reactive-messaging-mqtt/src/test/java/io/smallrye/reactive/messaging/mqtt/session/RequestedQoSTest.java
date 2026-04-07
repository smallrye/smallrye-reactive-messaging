package io.smallrye.reactive.messaging.mqtt.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RequestedQoSTest {

    @Test
    void valueOfZeroReturnsQOS0() {
        assertThat(RequestedQoS.valueOf(0)).isEqualTo(RequestedQoS.QOS_0);
    }

    @Test
    void valueOfOneReturnsQOS1() {
        assertThat(RequestedQoS.valueOf(1)).isEqualTo(RequestedQoS.QOS_1);
    }

    @Test
    void valueOfNullReturnsNull() {
        assertThat(RequestedQoS.valueOf((Integer) null)).isNull();
    }

    @Test
    void valueOfTwoReturnsQOS2() {
        assertThat(RequestedQoS.valueOf(2)).isEqualTo(RequestedQoS.QOS_2);
    }

    @Test
    void valueOfInvalidThrows() {
        assertThatThrownBy(() -> RequestedQoS.valueOf(3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toIntegerQOS0() {
        assertThat(RequestedQoS.QOS_0.toInteger()).isEqualTo(0);
    }

    @Test
    void toIntegerQOS1() {
        assertThat(RequestedQoS.QOS_1.toInteger()).isEqualTo(1);
    }

    @Test
    void toIntegerQOS2() {
        assertThat(RequestedQoS.QOS_2.toInteger()).isEqualTo(2);
    }
}
