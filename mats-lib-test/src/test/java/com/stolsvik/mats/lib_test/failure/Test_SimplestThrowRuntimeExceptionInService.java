package com.stolsvik.mats.lib_test.failure;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.lib_test.MatsBasicTest;

/**
 * Tests the simplest failure in a single-stage service: A single-stage endpoint is invoked from the Initiator, but the
 * service throws a {@link RuntimeException}, which should put the message on the MQ DLQ for that endpoint's queue after
 * the MQ has retried its configured number of times (in test there is one initial delivery, and one retry).
 * <p>
 * ASCII-artsy, it looks like this <i>(note that we do not bother setting up the Terminator)</i>:
 *
 * <pre>
 * [Initiator]
 *     [Service] - throws Exception, message ends up on DLQ after MQ retries.
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_SimplestThrowRuntimeExceptionInService extends MatsBasicTest {
    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    throw new RuntimeException("Should send message to DLQ after retries.");
                });
    }

    @Test
    public void doTest() throws InterruptedException {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR)
                        .request(dto, sto));

        // Wait for the DLQ
        MatsTrace dlqMatsTrace = matsRule.getDlqMessage(SERVICE);
        Assert.assertNotNull(dlqMatsTrace);
        Assert.assertEquals(SERVICE, dlqMatsTrace.getCurrentCall().getTo());
    }
}
