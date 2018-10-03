package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the Publish/Subscribe functionality: Sets up <i>two</i> instances of a SubscriptionTerminator to the same
 * endpointId, and then an initiator publishes a message to that endpointId. Due to the Pub/Sub nature, both of the
 * SubscriptionTerminators will get the message.
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 *                     [Initiator]  - init publish
 * [SubscriptionTerminator_1][SubscriptionTerminator_2] <i>(both receives the message)</i>
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_SimplePublishSubscribe extends MatsBasicTest {

    private MatsTestLatch matsTestLatch2 = new MatsTestLatch();

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().subscriptionTerminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("SUBSCRIPTION TERMINATOR 1 MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });
        matsRule.getMatsFactory().subscriptionTerminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("SUBSCRIPTION TERMINATOR 2 MatsTrace:\n" + context.toString());
                    matsTestLatch2.resolve(dto, sto);
                });

        // Nap for a small while, due to the nature of Pub/Sub: If the listeners are not up when the message is sent,
        // then they will not get the message.
        takeNap(100);
    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().createInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        .publish(dto, sto));

        // Wait synchronously for both terminators to finish.
        Result<DataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(dto, result.getData());
        Assert.assertEquals(sto, result.getState());

        Result<DataTO, StateTO> result2 = matsTestLatch2.waitForResult();
        Assert.assertEquals(dto, result2.getData());
        Assert.assertEquals(sto, result2.getState());
    }

}
