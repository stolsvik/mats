package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the "next stage" functionality: A three-stage service is set, where init-stage calls next to stage1, that calls
 * next to state2. A Terminator is set up. Then an initiator does a request to the service, setting replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]             - init request
 *     [Service S0 - init] - next
 *     [Service S1]        - next
 *     [Service S2 - last] - reply
 * [Terminator]
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_MutilStageNext extends MatsBasicTest {
    @Before
    public void setupMultiStageService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE, StateTO.class, DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(0, 0), sto);
            sto.number1 = Integer.MAX_VALUE;
            sto.number2 = Math.E;
            context.next(new DataTO(dto.number * 2, dto.string + ":InitialStage"));
        });
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(Integer.MAX_VALUE, Math.E), sto);
            sto.number1 = Integer.MIN_VALUE;
            sto.number2 = Math.PI;
            context.next(new DataTO(dto.number * 3, dto.string + ":Stage1"));
        });
        ep.lastStage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(Integer.MIN_VALUE, Math.PI), sto);
            return new DataTO(dto.number * 5, dto.string + ":ReplyStage");
        });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() {
        StateTO sto = new StateTO(420, 420.024);
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsFactory().createInitiator().initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for terminator to finish.
        Result<DataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2 * 3 * 5,
                dto.string + ":InitialStage" + ":Stage1" + ":ReplyStage"),
                result.getData());
    }
}
