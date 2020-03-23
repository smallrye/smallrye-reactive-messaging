package merge;

import io.smallrye.reactive.messaging.annotations.Merge;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class MergeExamples {

    // tag::chain[]
    @Incoming("in1")
    @Outgoing("out")
    public int increment(int i) {
        return i + 1;
    }

    @Incoming("in2")
    @Outgoing("out")
    public int multiply(int i) {
        return i * 2;
    }

    @Incoming("out")
    @Merge
    public void getAll(int i) {
        //...
    }

    // end::chain[]

}
