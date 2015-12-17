package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the simplest functionality: Sets up a Terminator endpoint, and then an initiator sends a message to that
 * endpoint.
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]
 * [Terminator]
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_SimplestSendReceive extends MatsBasicTest {
    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        .send(dto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(dto, result.getData());
    }
}
