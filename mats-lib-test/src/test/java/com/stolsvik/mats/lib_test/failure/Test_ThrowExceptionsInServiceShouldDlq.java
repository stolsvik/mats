package com.stolsvik.mats.lib_test.failure;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsDbTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests 2 scenarios:
 * <ol>
 * <li>The simplest failure in a single-stage service: A single-stage endpoint is invoked from the Initiator, but the
 * service throws a {@link RuntimeException}, which should put the message on the MQ DLQ for that endpoint's queue after
 * the MQ has retried its configured number of times (in test there is one initial delivery, and one retry).</li>
 * <li>The special "insta-DLQ" feature: A single-stage endpoint is invoked from the Initiator, but the service throws a
 * {@link MatsRefuseMessageException}, which should put the message on the MQ DLQ for that endpoint's queue right away,
 * without retries.</li>
 * </ol>
 * <p/>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]   - request
 *     [Service] - throws RuntimeException or MatsRefuseMessageException, message ends up on DLQ (after MQ retries).
 * [Terminator]  - <i>does not get message!</i>
 * </pre>
 *
 * @author Endre Stølsvik - 2015 + 2019-09-21 21:50 - http://endre.stolsvik.com
 */
public class Test_ThrowExceptionsInServiceShouldDlq extends MatsDbTest {

    private volatile AtomicInteger _serviceInvocations;

    @Before
    public void setupServiceAndTerminator() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    _serviceInvocations.incrementAndGet();
                    if (dto.string.equals("THROW RUNTIME")) {
                        context.reply(dto);
                        throw new RuntimeException("Should send message to DLQ after retries.");
                    }
                    if (dto.string.equals("THROW MATSREFUSE")) {
                        context.reply(dto);
                        throw new MatsRefuseMessageException("Should send message directly to DLQ, w/o retries.");
                    }
                    return dto;
                });

        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    matsTestLatch.resolve(context, sto, dto);
                });
    }

    @Test
    public void throwRuntimeExceptionInStageShouldRedeliverAndDlq() {
        doTest("THROW RUNTIME", 2, true);
    }

    @Test
    public void throwMatsRefuseExceptionInStageShoudInstaDlq() {
        doTest("THROW MATSREFUSE", 1, true);
    }

    @Test
    public void checkTestInfrastructure() {
        DataTO dto = doTest("No Throwing, please!", 1, false);

        // Wait for the reply that the TERMINATOR gets
        Result<StateTO, DataTO> reply = matsTestLatch.waitForResult();

        // Assert that the TERMINATOR got what we expected.
        Assert.assertEquals(dto, reply.getData());
        Assert.assertEquals(new StateTO(420, 420.024), reply.getState());
    }

    public DataTO doTest(String sendString, int expectedInvocationCount, boolean expectDlq) {
        _serviceInvocations = new AtomicInteger();
        DataTO dto = new DataTO(42, sendString);
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        if (expectDlq) {
            // Wait for the DLQ
            MatsTrace dlqMatsTrace = matsRule.getDlqMessage(SERVICE);
            Assert.assertNotNull(dlqMatsTrace);
            Assert.assertEquals(SERVICE, dlqMatsTrace.getCurrentCall().getTo().getId());

            // Assert that we got the expected number of invocations
            Assert.assertEquals(expectedInvocationCount, _serviceInvocations.get());

            // Assert that the reply was not received by terminator
            try {
                matsTestLatch.waitForResult(250);
            }
            catch (AssertionError ae) {
                log.info("Got the expected AssertionError, meaning that the TERMINATOR did not get a message, good!");
                return null;
            }

            Assert.fail("The TERMINATOR actually received the reply, while it should have!");
        }
        return dto;
    }
}
