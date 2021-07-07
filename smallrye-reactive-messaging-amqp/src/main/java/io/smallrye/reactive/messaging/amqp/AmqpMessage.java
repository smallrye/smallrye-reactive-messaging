package io.smallrye.reactive.messaging.amqp;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageError;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.amqp.fault.AmqpFailureHandler;
import io.smallrye.reactive.messaging.amqp.tracing.HeaderExtractAdapter;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.buffer.Buffer;

public class AmqpMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    protected static final String APPLICATION_JSON = "application/json";
    protected final io.vertx.amqp.AmqpMessage message;
    protected Metadata metadata;
    protected final IncomingAmqpMetadata amqpMetadata;
    private final Context context;
    protected final AmqpFailureHandler onNack;

    @Deprecated
    public static <T> AmqpMessageBuilder<T> builder() {
        return new AmqpMessageBuilder<>();
    }

    public AmqpMessage(io.vertx.mutiny.amqp.AmqpMessage delegate, Context context, AmqpFailureHandler onNack,
            Boolean tracingEnabled) {
        this(delegate.getDelegate(), context, onNack, tracingEnabled);
    }

    public AmqpMessage(io.vertx.amqp.AmqpMessage msg, Context context, AmqpFailureHandler onNack, Boolean tracingEnabled) {
        this.message = msg;
        this.context = context;
        this.amqpMetadata = new IncomingAmqpMetadata(this.message);
        this.onNack = onNack;

        ArrayList<Object> meta = new ArrayList<>();
        meta.add(amqpMetadata);

        if (tracingEnabled) {
            TracingMetadata tracingMetadata = TracingMetadata.empty();
            if (msg.applicationProperties() != null) {
                // Read tracing headers
                io.opentelemetry.context.Context otelContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                        .extract(io.opentelemetry.context.Context.root(), msg.applicationProperties(),
                                HeaderExtractAdapter.GETTER);
                tracingMetadata = TracingMetadata.withPrevious(otelContext);
            }

            meta.add(tracingMetadata);
        }

        this.metadata = Metadata.from(meta);
    }

    @Override
    public CompletionStage<Void> ack() {
        // We must switch to the context having created the message.
        // This context is passed when this instance of message is created.
        // It's more a Vert.x AMQP client issue which should ensure calling `accepted` on the right context.
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.context.runOnContext(() -> {
            this.message.accepted();
            future.complete(null);
        });
        return future;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return onNack.handle(this, context, reason);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getPayload() {
        // Throw a class cass exception if it cannot be converted.
        return (T) convert(message);
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    private Object convert(io.vertx.amqp.AmqpMessage msg) {
        Object body = msg.unwrap().getBody();
        if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value instanceof Binary) {
                Binary bin = (Binary) value;
                byte[] bytes = new byte[bin.getLength()];
                System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());
                return bytes;
            }
            return value;
        }

        if (body instanceof AmqpSequence) {
            return ((AmqpSequence) body).getValue();
        }

        if (body instanceof Data) {
            Binary bin = ((Data) body).getValue();
            byte[] bytes = new byte[bin.getLength()];
            System.arraycopy(bin.getArray(), bin.getArrayOffset(), bytes, 0, bin.getLength());

            if (APPLICATION_JSON.equalsIgnoreCase(msg.contentType())) {
                return Buffer.buffer(bytes).toJson();
            }
            return bytes;
        }

        return body;
    }

    public Message unwrap() {
        return message.unwrap();
    }

    public boolean isDurable() {
        return amqpMetadata.isDurable();
    }

    public long getDeliveryCount() {
        return amqpMetadata.getDeliveryCount();
    }

    public int getPriority() {
        return amqpMetadata.getPriority();
    }

    public long getTtl() {
        return amqpMetadata.getTtl();
    }

    public Object getMessageId() {
        return amqpMetadata.getId();
    }

    public long getGroupSequence() {
        return amqpMetadata.getGroupSequence();
    }

    public long getCreationTime() {
        return amqpMetadata.getCreationTime();
    }

    public String getAddress() {
        return amqpMetadata.getAddress();
    }

    public String getGroupId() {
        return amqpMetadata.getGroupId();
    }

    public String getContentType() {
        return amqpMetadata.getContentType();
    }

    public long getExpiryTime() {
        return amqpMetadata.getExpiryTime();
    }

    public Object getCorrelationId() {
        return amqpMetadata.getCorrelationId();
    }

    public String getContentEncoding() {
        return amqpMetadata.getContentEncoding();
    }

    public String getSubject() {
        return amqpMetadata.getSubject();
    }

    public JsonObject getApplicationProperties() {
        return amqpMetadata.getProperties();
    }

    public Section getBody() {
        return message.unwrap().getBody();
    }

    public MessageError getError() {
        return message.unwrap().getError();
    }

    public io.vertx.mutiny.amqp.AmqpMessage getAmqpMessage() {
        return new io.vertx.mutiny.amqp.AmqpMessage(message);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }

    public synchronized void injectTracingMetadata(TracingMetadata tracingMetadata) {
        metadata = metadata.with(tracingMetadata);
    }
}
