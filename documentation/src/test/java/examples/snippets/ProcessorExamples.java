package examples.snippets;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

@ApplicationScoped
public class ProcessorExamples {

    public static void main(String[] args) {
        SeContainerInitializer.newInstance().initialize();
    }

    @Outgoing("proc-a")
    public PublisherBuilder<String> source() {
        return ReactiveStreams.of("a", "b", "c", "d");
    }

    @Incoming("proc-d")
    public void sink(String input) {
        System.out.println(">> " + input);
    }

    // tag::processor[]
    @Incoming("proc-a")
    @Outgoing("proc-b")
    public PublisherBuilder<String> duplicate(String input) {
        return ReactiveStreams.of(input, input);
    }
    // end::processor[]

    // tag::filtering-processor[]
    @Incoming("proc-b")
    @Outgoing("proc-c")
    public PublisherBuilder<String> removeC(String input) {
        return ReactiveStreams.of(input)
                .filter(s -> !s.equalsIgnoreCase("c"))
                .map(String::toUpperCase);
    }
    // end::filtering-processor[]

    // tag::return-processor[]
    @Incoming("proc-c")
    @Outgoing("proc-d")
    public ProcessorBuilder<String, String> process() {
        return ReactiveStreams.<String> builder()
                .map(s -> "-" + s + "-");
    }
    // end::return-processor[]

}
