package io.smallrye.reactive.messaging.tck.incoming;

import static io.smallrye.reactive.messaging.tck.incoming.Bean.NON_PARALLEL;
import static io.smallrye.reactive.messaging.tck.incoming.Bean.NON_VOID_METHOD;
import static io.smallrye.reactive.messaging.tck.incoming.Bean.SYNC_FAILING;
import static io.smallrye.reactive.messaging.tck.incoming.Bean.VOID_METHOD;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBase;
import io.smallrye.reactive.messaging.tck.MessagingManager;
import io.smallrye.reactive.messaging.tck.MockPayload;
import io.smallrye.reactive.messaging.tck.MockedReceiver;

@Ignore("Work on the TCK")
public class IncomingTest extends WeldTestBase {

    @Override
    public List<Class> getBeans() {
        return Arrays.asList(
                Bean.class,
                MessagingManager.class);
    }

    // Used topic: VOID_METHOD
    @Test
    public void simpleCompletionStageVoidMethodShouldProcessMessages() {
        MessagingManager manager = init();

        MockedReceiver<MockPayload> receiver = manager.getReceiver(VOID_METHOD);
        MockPayload msg1 = new MockPayload("mock", 1);
        MockPayload msg2 = new MockPayload("mock", 2);
        MockPayload msg3 = new MockPayload("mock", 3);

        manager.sendPayloads(VOID_METHOD, msg1, msg2);

        receiver.expectNextMessageWithPayload(msg1);
        receiver.expectNextMessageWithPayload(msg2);
        receiver.expectNoMessages("Didn't expect a message because didn't send one.");
        manager.sendPayloads(VOID_METHOD, msg3);
        receiver.expectNextMessageWithPayload(msg3);
    }

    private MessagingManager init() {
        initialize();
        return container.select(MessagingManager.class).get();
    }

    @Test
    public void completionStageMethodShouldNotProcessMessagesInParallel() {
        initialize();
        MessagingManager manager = container.select(MessagingManager.class).get();
        Bean bean = container.select(Bean.class).get();

        MockedReceiver<MockPayload> receiver = manager.getReceiver(NON_PARALLEL);
        MockPayload msg1 = new MockPayload("mock", 1);
        MockPayload msg2 = new MockPayload("mock", 2);
        MockPayload msg3 = new MockPayload("mock", 3);

        manager.sendPayloads(NON_PARALLEL, msg1, msg2);

        receiver.expectNextMessageWithPayload(msg1);
        receiver.expectNoMessages("Did not redeem future from previous message yet.");
        bean.getFutures().removeFirst().complete(null);
        receiver.expectNextMessageWithPayload(msg2);
        receiver.expectNoMessages("Did not redeem future from previous message yet.");
        bean.getFutures().removeFirst().complete(null);
        manager.sendPayloads(NON_PARALLEL, msg3);
        receiver.expectNextMessageWithPayload(msg3);
        bean.getFutures().removeFirst().complete(null);
    }

    @Test
    public void completionStageNonVoidMethodShouldIgnoreReturnValue() {
        MessagingManager manager = init();

        MockedReceiver<MockPayload> receiver = manager.getReceiver(NON_VOID_METHOD);
        MockPayload msg1 = new MockPayload("mock", 1);
        MockPayload msg2 = new MockPayload("mock", 2);

        manager.sendPayloads(NON_VOID_METHOD, msg1, msg2);

        receiver.expectNextMessageWithPayload(msg1);
        receiver.expectNextMessageWithPayload(msg2);
        receiver.expectNoMessages("Didn't expect a message because didn't send one.");
    }

    @Test
    @Ignore("Discuss retry")
    public void completionStageMethodShouldRetryMessagesThatFailSynchronously() {
        initialize();
        MessagingManager manager = container.select(MessagingManager.class).get();
        Bean bean = container.select(Bean.class).get();

        MockedReceiver<MockPayload> receiver = manager.getReceiver(SYNC_FAILING);
        MockPayload msg1 = new MockPayload("success", 1);
        MockPayload msg2 = new MockPayload("fail", 2);
        MockPayload msg3 = new MockPayload("success", 3);

        manager.sendPayloads(SYNC_FAILING, msg1, msg2, msg3);
        // We should receive the fail message once, then failed should be true, then we should receive it again,
        // followed by the next message.
        receiver.expectNextMessageWithPayload(msg1);
        receiver.expectNextMessageWithPayload(msg2);
        assertTrue("Sync was not failed", bean.getSyncFailed().get());
        receiver.expectNextMessageWithPayload(msg2);
        receiver.expectNextMessageWithPayload(msg3);
    }

    //  @Test
    //  public void completionStageMethodShouldRetryMessagesThatFailAsynchronously() {
    //    SeContainer container = initializer.initialize();
    //    MessagingManager manager = container.select(MessagingManager.class).get();
    //    Bean bean = container.select(Bean.class).get();
    //
    //    MockedReceiver<MockPayload> receiver = manager.getReceiver(ASYNC_FAILING);
    //    MockPayload msg1 = new MockPayload("success", 1);
    //    MockPayload msg2 = new MockPayload("fail", 2);
    //    MockPayload msg3 = new MockPayload("success", 3);
    //
    //    manager.sendPayloads(ASYNC_FAILING, msg1, msg2, msg3);
    //    // We should receive the fail message once, then failed should be true, then we should receive it again,
    //    // followed by the next message.
    //    receiver.expectNextMessageWithPayload(msg1);
    //    receiver.expectNextMessageWithPayload(msg2);
    //    assertTrue("Async was not failed", bean.getAsyncFailed().get());
    //    receiver.expectNextMessageWithPayload(msg2);
    //    receiver.expectNextMessageWithPayload(msg3);
    //  }
    //
    //  @Test
    //  @Ignore("Check TCK")
    //  public void completionStageMethodShouldNotAutomaticallyAcknowledgeWrappedMessages() {
    //    SeContainer container = initializer.initialize();
    //    MessagingManager manager = container.select(MessagingManager.class).get();
    //    Bean bean = container.select(Bean.class).get();
    //
    //    MockedReceiver<MockPayload> receiver = manager.getReceiver(WRAPPED_MESSAGE);
    //    MockPayload msg1 = new MockPayload("acknowledged", 1);
    //    MockPayload msg2 = new MockPayload("unacknowledged", 2);
    //    MockPayload msg3 = new MockPayload("fail", 3);
    //    MockPayload msg4 = new MockPayload("success", 4);
    //
    //    manager.sendPayloads(WRAPPED_MESSAGE, msg1, msg2, msg3, msg4);
    //    // First one should be acknowledge. The second one gets processed successfully, but first time, doesn't ack.
    //    // Third one fails on the first attempt, on the second acks. Fourth one always acks.
    //    receiver.expectNextMessageWithPayload(msg1);
    //    receiver.expectNextMessageWithPayload(msg2);
    //    receiver.expectNextMessageWithPayload(msg3);
    //    assertTrue("Wrapped was not failed", bean.getWrappedFailed().get());
    //    receiver.expectNextMessageWithPayload(msg2);
    //    receiver.expectNextMessageWithPayload(msg3);
    //    receiver.expectNextMessageWithPayload(msg4);
    //  }
}
