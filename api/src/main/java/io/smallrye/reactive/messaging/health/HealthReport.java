package io.smallrye.reactive.messaging.health;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represent an health status and its set of attached data.
 */
public class HealthReport {

    /**
     * A report used when there are no channels.
     */
    public static final HealthReport OK_INSTANCE = new HealthReport(Collections.emptyList());

    /**
     * The general status. {@code true} is everything is fine, {@code false} otherwise
     */
    private final boolean ok;

    /**
     * Contains the details for each channel.
     */
    private final List<ChannelInfo> channels;

    public HealthReport(List<ChannelInfo> channels) {
        this.channels = Collections.unmodifiableList(channels);
        for (ChannelInfo info : channels) {
            if (!info.isOk()) {
                ok = false;
                return;
            }
        }
        ok = true;
    }

    public boolean isOk() {
        return ok;
    }

    public List<ChannelInfo> getChannels() {
        return channels;
    }

    public static HealthReportBuilder builder() {
        return new HealthReportBuilder();
    }

    /**
     * Structure storing the health detail of a specific channel.
     */
    public static class ChannelInfo {

        /**
         * The name of the channel, must not be {@code null}
         */
        private final String channel;

        /**
         * An optional message.
         */
        private final String message;

        /**
         * Whether the channel is ready or alive (depending on the check).
         */
        private final boolean ok;

        public ChannelInfo(String channel, boolean ok, String message) {
            this.channel = channel;
            this.message = message;
            this.ok = ok;
        }

        public ChannelInfo(String channel, boolean ok) {
            this(channel, ok, null);
        }

        public String getChannel() {
            return channel;
        }

        public String getMessage() {
            return message;
        }

        public boolean isOk() {
            return ok;
        }
    }

    /**
     * A builder to ease the creation of {@link HealthReport}.
     * Instances are not thread-safe.
     */
    public static class HealthReportBuilder {

        private final List<ChannelInfo> channels = new ArrayList<>();

        private HealthReportBuilder() {
            // avoid direct instantiation.
        }

        /**
         * Adds a channel info to the report.
         *
         * @param info the info, must not be {@code null}
         * @return this builder
         */
        public HealthReportBuilder add(ChannelInfo info) {
            this.channels.add(info);
            return this;
        }

        /**
         * Adds a channel info to the report.
         *
         * @param channel the channel, must not be {@code null}
         * @param ok if the channel is ok
         * @return this builder
         */
        public HealthReportBuilder add(String channel, boolean ok) {
            return add(new ChannelInfo(channel, ok));
        }

        /**
         * Adds a channel info to the report.
         *
         * @param channel the channel, must not be {@code null}
         * @param ok if the channel is ok
         * @param message an optional message
         * @return this builder
         */
        public HealthReportBuilder add(String channel, boolean ok, String message) {
            return add(new ChannelInfo(channel, ok, message));
        }

        /**
         * @return the built report
         */
        public HealthReport build() {
            return new HealthReport(channels);
        }
    }

}
