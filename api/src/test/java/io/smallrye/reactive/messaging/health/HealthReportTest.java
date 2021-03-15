package io.smallrye.reactive.messaging.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class HealthReportTest {

    @Test
    public void testBuilderAndReport() {
        assertThat(HealthReport.builder().add("my-channel", false).build().isOk()).isFalse();
        assertThat(HealthReport.builder().add("my-channel", true).build().isOk()).isTrue();

        HealthReport report = HealthReport.builder()
                .add("my-channel-1", true, "everything ok")
                .add("my-channel-2", false, "oh no!")
                .add("my-channel-3", true)
                .build();

        assertThat(report.isOk()).isFalse();
        assertThat(report.getChannels()).hasSize(3);
        assertThat(report.getChannels().get(0).getChannel()).isEqualTo("my-channel-1");
        assertThat(report.getChannels().get(0).getMessage()).isEqualTo("everything ok");
        assertThat(report.getChannels().get(0).isOk()).isTrue();
        assertThat(report.getChannels().get(1).getChannel()).isEqualTo("my-channel-2");
        assertThat(report.getChannels().get(1).getMessage()).isEqualTo("oh no!");
        assertThat(report.getChannels().get(1).isOk()).isFalse();
        assertThat(report.getChannels().get(2).getChannel()).isEqualTo("my-channel-3");
        assertThat(report.getChannels().get(2).getMessage()).isNull();
        assertThat(report.getChannels().get(2).isOk()).isTrue();

        HealthReport report2 = HealthReport.builder()
                .add("my-channel-1", true, "everything ok")
                .add("my-channel-2", true, "back on track")
                .add("my-channel-3", true)
                .build();

        assertThat(report2.isOk()).isTrue();
        assertThat(report2.getChannels()).hasSize(3);
    }

    @Test
    public void testOkInstance() {
        assertThat(HealthReport.OK_INSTANCE.isOk()).isTrue();
        assertThat(HealthReport.OK_INSTANCE.getChannels()).isEmpty();

        assertThatThrownBy(() -> HealthReport.OK_INSTANCE.getChannels().add(new HealthReport.ChannelInfo("boom", false)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
