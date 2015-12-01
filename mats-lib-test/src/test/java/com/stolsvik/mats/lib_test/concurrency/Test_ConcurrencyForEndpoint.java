package com.stolsvik.mats.lib_test.concurrency;

import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsFactory;

/**
 * Tests concurrency by sending 8 requests to a service, where the processing takes 500 ms, but where the concurrency is
 * also set to 8, thereby all those 8 requests should go through in just a tad over 500 ms, not 4000 ms as if there was
 * only 1 processor. Implicitly tests "lambdaconfig" for endpoint.
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator] x 1, firing off 8 requests.
 *     [Service] x 8 StageProcessors (sleeping 500 ms)
 * [Terminator] x 1 StageProcessor, getting all the 8 replies, counting down a 8-latch.
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_ConcurrencyForEndpoint extends ATest_AbstractConcurrency {

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (endpointConfig) -> endpointConfig.setConcurrency(CONCURRENCY_TEST),
                MatsFactory.NO_CONFIG,
                (context, dto) -> {
                    // Emulate some lengthy processing...
                    takeNap(PROCESSING_TIME);
                    return new DataTO(dto.number * 2, dto.string + ":FromService:" + (int) dto.number);
                });
    }

    @Test
    public void doTest() throws InterruptedException {
        performTest(2, "TheAnswer:FromService:");
    }
}
