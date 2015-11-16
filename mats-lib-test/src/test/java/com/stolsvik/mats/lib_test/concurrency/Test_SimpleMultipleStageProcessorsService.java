package com.stolsvik.mats.lib_test.concurrency;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.AMatsTest;

/**
 * Tests concurrency by sending 8 requests to a service, where the processing takes 500 ms, but where the concurrency is
 * also set to 8, thereby all those 8 requests should go through in just a tad over 500 ms, not 4000 ms as if there was
 * only 1 processor. Implicitly tests "lambdaconfig" for single-stage.
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator] x 1 StageProcessor
 *     [Service] x 8 StageProcessors
 * [Terminator] x 1 StageProcessor
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_SimpleMultipleStageProcessorsService extends AMatsTest {

    private static final int CONCURRENCY_TEST = 8;

    private static final int PROCESSING_TIME = 500;

    private CountDownLatch _latch = new CountDownLatch(CONCURRENCY_TEST);

    private Map<Integer, DataTO> _map = new ConcurrentHashMap<>();

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (endpointConfig) -> endpointConfig.setConcurrency(CONCURRENCY_TEST),
                (context, dto) -> {
                    // :: "Emulate" some lengthy processing...
                    sleep(PROCESSING_TIME);
                    return new DataTO(dto.number * 2, dto.string + ":FromService:" + (int) dto.number);
                });

    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class, (context, dto, sto) -> {
            _map.put(Integer.valueOf(sto.number1), dto);
            _latch.countDown();
        });
    }

    @Test
    public void doTest() throws InterruptedException {
        DataTO[] requests = new DataTO[CONCURRENCY_TEST];
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            for (int i = 0; i < CONCURRENCY_TEST; i++) {
                DataTO dto = new DataTO(i, "TheAnswer");
                StateTO sto = new StateTO(i, i);
                requests[i] = dto;
                msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR)
                        .request(dto, sto);
            }
        });

        // Wait synchronously for terminator to finish.
        boolean gotToZero = _latch.await((int) (PROCESSING_TIME * 1.5), TimeUnit.MILLISECONDS);
        Assert.assertTrue("The CountDownLatch did not reach zero in " + (PROCESSING_TIME * 1.5) + " ms.", gotToZero);

        // :: Assert the processed data
        for (int i = 0; i < CONCURRENCY_TEST; i++) {
            DataTO dto = _map.get(Integer.valueOf(i));
            Assert.assertEquals(i * 2, dto.number, 0);
            Assert.assertEquals("TheAnswer:FromService:" + i, dto.string);
        }
    }
}
