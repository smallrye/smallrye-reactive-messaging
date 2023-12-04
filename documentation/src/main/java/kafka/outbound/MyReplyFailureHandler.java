package kafka.outbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.Header;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.reply.ReplyFailureHandler;

@ApplicationScoped
@Identifier("my-reply-error")
public class MyReplyFailureHandler implements ReplyFailureHandler {

    @Override
    public Throwable handleReply(KafkaRecord<?, ?> replyRecord) {
        Header header = replyRecord.getHeaders().lastHeader("REPLY_ERROR");
        if (header != null) {
            return new IllegalArgumentException(new String(header.value()));
        }
        return null;
    }
}
