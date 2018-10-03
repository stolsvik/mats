package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the simplest request functionality: A single-stage service is set up. A Terminator is set up. Then an initiator
 * does a request to the service, setting replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]   - request
 *     [Service] - reply
 * [Terminator]
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_SimpleReplyToSubscription extends MatsBasicTest {
    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    log.debug("MatsTrace at service:\n" + context.toString());
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().subscriptionTerminator(TERMINATOR + "_Subscription", DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("SUBSCRIPTION TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });

    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(1337, "Elite");
        StateTO sto = new StateTO(999, 333.333);
        matsRule.getMatsFactory().createInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyToSubscription(TERMINATOR + "_Subscription", sto)
                        .request(dto));

        // Wait synchronously for terminator to finish.
        Result<DataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());
    }
}
