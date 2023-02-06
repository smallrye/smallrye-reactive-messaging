package kafka.outbound;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
@Identifier("my-producer-interceptor")
public class ProducerInterceptorBeanExample implements ProducerInterceptor<Integer, String> {

    @Override
    public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> producerRecord) {
        // called before send
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // called on send acknowledgement callback
    }

    @Override
    public void close() {
        // called on client close
    }

    @Override
    public void configure(Map<String, ?> map) {
        // called on client configuration
    }
}
