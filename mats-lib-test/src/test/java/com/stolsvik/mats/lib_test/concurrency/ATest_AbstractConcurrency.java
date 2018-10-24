package com.stolsvik.mats.lib_test.concurrency;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;

/**
 * Abstract class for concurrency tests.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class ATest_AbstractConcurrency extends MatsBasicTest {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    protected static final int CONCURRENCY_TEST = 8;

    protected static final int PROCESSING_TIME = 500;

    private CountDownLatch _latch = new CountDownLatch(CONCURRENCY_TEST);

    private Map<Integer, DataTO> _map = new ConcurrentHashMap<>();

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class, (context, sto, dto) -> {
            _map.put(Integer.valueOf(sto.number1), dto);
            _latch.countDown();
        });
    }

    protected void performTest(double expectedMultiple, String expectedString) throws InterruptedException {
        /*
         * Sometimes get problem that all the processors has not gotten into consumer.receive()-call before we fire off
         * the 8 messages and the first processor gets a message. Evidently the first processors then get two of the
         * messages (the one that gets a message before the latecomer has gotten into receive()), while the latecomer
         * gets none, and then the test fails.
         *
         * Remedy by napping a little before firing off the messages, hoping that all the StageProcessors gets one
         * message each, which is a requirement for the test to pass.
         */
        takeNap(PROCESSING_TIME / 4);

        // .. Now fire off the messages.
        matsRule.getMatsFactory().createInitiator().initiateUnchecked((msg) -> {
            for (int i = 0; i < CONCURRENCY_TEST; i++) {
                DataTO dto = new DataTO(i, "TheAnswer");
                StateTO sto = new StateTO(i, i);
                msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto);
            }
        });

        // Wait synchronously for all messages to reach terminator
        long maxWait = (long) (PROCESSING_TIME * 1.3);
        long startMillis = System.currentTimeMillis();
        boolean gotToZero = _latch.await((long) (PROCESSING_TIME * CONCURRENCY_TEST * 1.5), TimeUnit.MILLISECONDS);
        long millisTaken = System.currentTimeMillis() - startMillis;
        Assert.assertTrue("The CountDownLatch did not reach zero.", gotToZero);
        Assert.assertTrue("The CountDownLatch did not reach zero in " + maxWait + " ms (took " + millisTaken + "ms).",
                millisTaken < maxWait);
        log.info("@@ Test passed - Waiting for " + CONCURRENCY_TEST + " messages took " + millisTaken + " ms.");

        // :: Assert the processed data
        for (int i = 0; i < CONCURRENCY_TEST; i++) {
            DataTO dto = _map.get(i);
            Assert.assertEquals(i * expectedMultiple, dto.number, 0);
            Assert.assertEquals(expectedString + i, dto.string);
        }
    }
}
