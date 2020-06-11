package com.stolsvik.mats.lib_test.lifecycle;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the ability to remove and endpoint and then register a new one with the same endpointId, by first having one
 * single-stage endpoint, doing a test-round, and then replacing it with another, and doing another.
 *
 * @author Endre Stølsvik 2020-06-11 15:08 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_RemoveEndpoint extends MatsBasicTest {
    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> new DataTO(dto.number * 2, dto.string + ":FromService"));
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                });

    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for terminator to finish.
        // NOTICE: The SERVICE endpoint multiplies by
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());

        // NOW, change the Endpoint!

        long nanos_StartRemove = System.nanoTime();

        // First find it
        Optional<MatsEndpoint<?, ?>> endpoint = matsRule.getMatsFactory().getEndpoint(SERVICE);
        if (!endpoint.isPresent()) {
            throw new AssertionError("Didn't find endpoint!");
        }
        // .. remove it
        endpoint.get().remove(1000);

        double msRemoveTaken = (System.nanoTime() - nanos_StartRemove) / 1_000_000d;

        // .. register a new one:
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, msg) -> new DataTO(msg.number * 4, msg.string + ":FromService"));

        double msRemoveAndAddTaken = (System.nanoTime() - nanos_StartRemove) / 1_000_000d;

        log.info("######## ENDPOINT REMOVED AND ADDED, remove time taken: [" + msRemoveTaken
                + " ms], total:[" + msRemoveAndAddTaken + " ms].");

        // .. and run a new test through
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for terminator to finish.
        result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 4, dto.string + ":FromService"), result.getData());

    }

}
