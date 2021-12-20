package merge;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.reactive.messaging.annotations.Merge;

public class MergeExamples {

    // <chain>
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

    // </chain>

}
