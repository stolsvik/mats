package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestHelp;
import com.stolsvik.mats.test.MatsTestLatch.Result;
import com.stolsvik.mats.test.junit.Rule_Mats;

/**
 * Tests that the Initiate TraceId Modifier Function works.
 * 
 * @author Endre Stølsvik 2021-04-12 21:15 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_InitiateTraceIdModifier {
    private static final Logger log = MatsTestHelp.getClassLogger();

    @ClassRule
    public static final Rule_Mats MATS = Rule_Mats.create();

    private static final String TERMINATOR = MatsTestHelp.terminator();

    @BeforeClass
    public static void setupTerminator() {
        MATS.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    MATS.getMatsTestLatch().resolve(context, sto, dto);
                });
    }

    @Test
    public void simpleModifyTraceId() {
        // :: Arrange
        MATS.getMatsFactory().getFactoryConfig().setInitiateTraceIdModifier((origTraceId) -> "Prefixed|" + origTraceId);

        // :: Act
        StateTO sto = new StateTO(7, 3.14);
        DataTO dto = new DataTO(42, "TheAnswer");
        String origTraceId = MatsTestHelp.traceId();
        MATS.getMatsInitiator().initiateUnchecked(
                (init) -> init
                        .traceId(origTraceId)
                        .from(MatsTestHelp.from("test"))
                        .to(TERMINATOR)
                        .send(dto, sto));

        // :: Assert
        Result<StateTO, DataTO> result = MATS.getMatsTestLatch().waitForResult();
        Assert.assertEquals(dto, result.getData());
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals("Prefixed|" + origTraceId, result.getContext().getTraceId());
    }

    @Test
    public void withinStageShouldNotModify() {
        // :: Arrange
        MATS.getMatsFactory().getFactoryConfig().setInitiateTraceIdModifier((origTraceId) -> "Prefixed|" + origTraceId);

        // Create a single-stage service
        String sendingTerminator = MatsTestHelp.service();
        MATS.getMatsFactory().terminator(sendingTerminator, StateTO.class, DataTO.class, (ctx, state, msg) -> {
            ctx.initiate(init -> init.traceId("WithinStage").to(TERMINATOR).send(msg, state));
        });

        // :: Act
        StateTO sto = new StateTO(7, 3.14);
        DataTO dto = new DataTO(42, "TheAnswer");
        String origTraceId = MatsTestHelp.traceId();
        MATS.getMatsInitiator().initiateUnchecked(
                (init) -> init
                        .traceId(origTraceId)
                        .from(MatsTestHelp.from("test"))
                        .to(sendingTerminator)
                        .send(dto, sto));

        // :: Assert
        Result<StateTO, DataTO> result = MATS.getMatsTestLatch().waitForResult();
        Assert.assertEquals(dto, result.getData());
        Assert.assertEquals(sto, result.getState());

        // NOTE: The TraceId was prefixed with "Prefixed|" at the "from the outside" initiation, but NOT when
        // initiated new message within Stage (otherwise, there would be 2x the prefix).
        Assert.assertEquals("Prefixed|" + origTraceId + "|WithinStage", result.getContext().getTraceId());
    }
}
