package com.stolsvik.mats.lib_test.basics;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the Time-To-Live feature, by sending 4 messages with TTL = 75, and then a "flushing" FINAL message without
 * setting the TTL. The service sleeps for 100 ms. The MatsBasicTest has a MatsFactory with concurrency = 1. Therefore,
 * only the first of the TTLed messages should come through, as the rest should have timed out when the service is ready
 * to accept them again. The FINAL message should come through anyway, since it does not have timeout. Therefore, the
 * expected number of delivered messages is 2. Also, a test of the "test infrastructure" is performed, by setting the
 * TTL for the 4 messages to 0, which is "forever", hence all should now be delivered, and the expected number of
 * delivered messages should then be 5.
 *
 * @author Endre Stølsvik 2019-08-25 22:40 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_TimeToLive extends MatsBasicTest {
    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    if ("DELAY".equals(dto.string)) {
                        try {
                            Thread.sleep(100);
                        }
                        catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    private AtomicInteger _numberOfMessages = new AtomicInteger();

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    _numberOfMessages.incrementAndGet();
                    if (dto.string.startsWith("FINAL")) {
                        matsTestLatch.resolve(sto, dto);
                    }
                });

    }

    @Test
    public void checkTestInfrastructure() {
        doTest(0, 5);
    }

    @Test
    public void testWithTimeToLive() {
        doTest(75, 2);
    }

    private void doTest(long timeToLive, int expectedMessages) {
        DataTO finalDto = new DataTO(42, "FINAL");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> {
                    // First send 4 messages with the specified TTL.
                    for (int i = 0; i < 4; i++) {
                        DataTO dto = new DataTO(i, "DELAY");
                        msg.traceId(randomId())
                                .from(INITIATOR)
                                .to(SERVICE)
                                .timeToLive(timeToLive)
                                .replyTo(TERMINATOR, sto)
                                .request(dto);
                    }
                    // Then send a "flushing" FINAL message, which is the one that resolves the latch.
                    // NOTE: This does NOT set the TTL, to test that the default is 0, even though we sat the TTL for
                    // the previous ones.
                    msg.traceId(randomId())
                            .from(INITIATOR)
                            .to(SERVICE)
                            .replyTo(TERMINATOR, sto)
                            .request(finalDto);
                });

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(finalDto.number * 2, finalDto.string + ":FromService"), result.getData());
        Assert.assertEquals(expectedMessages, _numberOfMessages.get());
    }

}