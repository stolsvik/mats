package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the initiation within a stage functionality: Three Terminators are set up: One for the normal service return,
 * and two extra for the initiations that are done within the service's single stage. The single-stage service is set up
 * - which initiates two new messages to the two extra Terminators, and returns a result to the normal Terminator. Then
 * an initiator does a request to the service, setting replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]   - request
 *     [Service] - reply + initiates to Terminator "stageInit1" and initiates to Terminator "stageInit2".
 * [Terminator]                        [Terminator "stageInit1"]                [Terminator "stageInit2"]
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_InitiateWithinStage extends MatsBasicTest {
    private MatsTestLatch matsTestLatch_stageInit1 = new MatsTestLatch();
    private MatsTestLatch matsTestLatch_stageInit2 = new MatsTestLatch();

    private volatile String _traceId;
    private volatile String _traceId_stageInit1;
    private volatile String _traceId_stageInit2;

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    // Fire off two new initiations to the two Terminators
                    context.initiate(
                            msg -> {
                        msg.traceId("subtraceId1")
                                .to(TERMINATOR + "_stageInit1")
                                .send(new DataTO(Math.E, "xyz"));
                        msg.traceId("subtraceId2")
                                .to(TERMINATOR + "_stageInit2")
                                .send(new DataTO(-Math.E, "abc"), new StateTO(Integer.MAX_VALUE, Math.PI));

                    });
                    // Return our result
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    @Before
    public void setupTerminators() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("Normal TERMINATOR MatsTrace:\n" + context.toString());
                    _traceId = context.getTraceId();
                    matsTestLatch.resolve(dto, sto);
                });
        matsRule.getMatsFactory().terminator(TERMINATOR + "_stageInit1", StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("StageInit1 TERMINATOR MatsTrace:\n" + context.toString());
                    _traceId_stageInit1 = context.getTraceId();
                    matsTestLatch_stageInit1.resolve(dto, sto);
                });
        matsRule.getMatsFactory().terminator(TERMINATOR + "_stageInit2", StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("StageInit2 TERMINATOR MatsTrace:\n" + context.toString());
                    _traceId_stageInit2 = context.getTraceId();
                    matsTestLatch_stageInit2.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        String randomId = randomId();
        matsRule.getMatsFactory().createInitiator().initiateUnchecked(
                msg -> {
                    msg.traceId(randomId)
                            .from(INITIATOR)
                            .to(SERVICE)
                            .replyTo(TERMINATOR, sto)
                            .request(dto);
                });

        // :: Wait synchronously for all three terminators to finish.
        // "Normal" Terminator from the service call
        Result<DataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());
        Assert.assertEquals(randomId, _traceId);

        // Terminator "stageInit1", for the first initiation within the service's stage
        Result<DataTO, StateTO> result_stageInit1 = matsTestLatch_stageInit1.waitForResult();
        Assert.assertEquals(new StateTO(0, 0), result_stageInit1.getState());
        Assert.assertEquals(new DataTO(Math.E, "xyz"), result_stageInit1.getData());
        Assert.assertEquals(randomId + "|subtraceId1", _traceId_stageInit1);

        // Terminator "stageInit2", for the second initiation within the service's stage
        Result<DataTO, StateTO> result_stageInit2 = matsTestLatch_stageInit2.waitForResult();
        Assert.assertEquals(new StateTO(Integer.MAX_VALUE, Math.PI), result_stageInit2.getState());
        Assert.assertEquals(new DataTO(-Math.E, "abc"), result_stageInit2.getData());
        Assert.assertEquals(randomId + "|subtraceId2", _traceId_stageInit2);
    }
}
