package com.stolsvik.mats.lib_test.basics;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.AMatsTest;
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
 *                     [Initiator]
 * [SubscriptionTerminator_1][SubscriptionTerminator_2] <i>(both receives the message)</i>
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_SimplePublishSubscribe extends AMatsTest {

    private MatsTestLatch matsTestLatch2 = new MatsTestLatch();

    @Before
    public void setupTerminator() throws InterruptedException {
        matsRule.getMatsFactory().subscriptionTerminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    matsTestLatch.resolve(dto, sto);
                });
        matsRule.getMatsFactory().subscriptionTerminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    matsTestLatch2.resolve(dto, sto);
                });

        // Sleep for a small while, due to the nature of Pub/Sub: If the listeners are not up when the message is sent,
        // then they will not get the message.
        Thread.sleep(100);
    }

    @Test
    public void doTest() throws JMSException, InterruptedException {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            msg.traceId(randomId()).from(INITIATOR).to(TERMINATOR).publish(dto, sto);
        });

        // Wait synchronously for both terminators to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(dto, result.getData());
        Assert.assertEquals(sto, result.getState());

        Result<StateTO, DataTO> result2 = matsTestLatch2.waitForResult();
        Assert.assertEquals(dto, result2.getData());
        Assert.assertEquals(sto, result2.getState());
    }

}
