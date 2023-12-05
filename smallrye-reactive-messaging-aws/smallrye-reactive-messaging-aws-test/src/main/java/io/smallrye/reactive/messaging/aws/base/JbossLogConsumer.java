package io.smallrye.reactive.messaging.aws.base;

import org.jboss.logging.Logger;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

public class JbossLogConsumer extends BaseConsumer<JbossLogConsumer> {

    private final Logger logger;

    private String prefix = "";

    public JbossLogConsumer(final Logger logger) {
        this.logger = logger;
    }

    public JbossLogConsumer withPrefix(String prefix) {
        this.prefix = "\uD83D\uDC33 [" + prefix + "] ";
        return this;
    }

    @Override
    public void accept(final OutputFrame outputFrame) {
        final OutputFrame.OutputType outputType = outputFrame.getType();

        String utf8String = outputFrame.getUtf8String();
        utf8String = utf8String.replaceAll("((\r?\n)|(\r))$", "");

        switch (outputType) {
            case END:
                break;
            case STDOUT:
                logger.infov("{0}{1}: {2}", prefix, outputType, utf8String);
                break;
            case STDERR:
                logger.errorv("{0}{1}: {2}", prefix, outputType, utf8String);
                break;
            default:
                throw new IllegalArgumentException("Unexpected outputType " + outputType);
        }
    }
}
