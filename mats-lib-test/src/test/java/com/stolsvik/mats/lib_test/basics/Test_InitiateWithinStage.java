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
 * Tests the initiation within a stage functionality: Four Terminators are set up: One for the normal service return,
 * two for the initiations that are done within the service's single stage, plus one addition for the initiation done
 * directly on the MatsFactory from within the Stage (NOT recommended way to code!). A single-stage service is set up -
 * which initiates three new messages to the three extra Terminators (2 x initiations on ProcessContext, and 1
 * initiation directly on the MatsFactory), and returns a result to the normal Terminator. Then an initiator does a
 * request to the service, setting replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]   - request
 *     [Service] - reply + sends to Terminator "stageInit1", to "stageInit2" and to "stageInit_withMatsFactory"
 * [Terminator]       +            [Terminator "stageInit1"] [T "stageInit2"]    [T "stageInit_withMatsFactory"]
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_InitiateWithinStage extends MatsBasicTest {
    private MatsTestLatch matsTestLatch_stageInit1 = new MatsTestLatch();
    private MatsTestLatch matsTestLatch_stageInit2 = new MatsTestLatch();
    private MatsTestLatch matsTestLatch_stageInit_withMatsFactory = new MatsTestLatch();

    private volatile String _traceId;
    private volatile String _traceId_stageInit1;
    private volatile String _traceId_stageInit2;
    private volatile String _traceId_stageInit_withMatsFactory;

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

                                // Initiate directly on MatsFactory, not the ProcessContext.
                                //
                                // NOTICE!!! THIS WILL HAPPEN OUTSIDE THE TRANSACTION DEMARCATION FOR THIS STAGE!
                                // That means that this 'send' basically happens immediately, and it will happen even
                                // if some exception is raised later in the code. It will not be committed nor rolled
                                // back along with the rest of operations happening in the stage - it is separate.
                                //
                                // It is very much like (actually: pretty much literally) getting a new Connection from
                                // a DataSource while already being within a transaction on an existing Connection:
                                // Anything happening on this new Connection is totally separate from the existing
                                // transaction demarcation.
                                //
                                // This also means that the send initiated on the MatsFactory will happen before the
                                // two sends initiated on the ProcessContext above, since this new transaction will
                                // commit before the transaction surrounding the Mats process lambda that we're within.
                                //
                                // Please check through the logs to see what happens.
                                //
                                // TAKEAWAY: This should not be a normal way to code! But it is possible to do.
                                matsRule.getMatsFactory().getDefaultInitiator().initiateUnchecked(init -> {
                                    init.traceId("New TraceId")
                                            .from("New Init")
                                            .to(TERMINATOR + "_stageInit_withMatsFactory")
                                            .send(new DataTO(Math.PI, "Endre"));
                                });

                            });
                    // Return our result
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    @Before
    public void setupTerminators() {
        // :: Termintor for the normal service REPLY
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("Normal TERMINATOR MatsTrace:\n" + context.toString());
                    _traceId = context.getTraceId();
                    matsTestLatch.resolve(sto, dto);
                });
        // :: Two terminators for the initiations within the stage executed on the ProcessContext of the stage
        matsRule.getMatsFactory().terminator(TERMINATOR + "_stageInit1", StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("StageInit1 TERMINATOR MatsTrace:\n" + context.toString());
                    _traceId_stageInit1 = context.getTraceId();
                    matsTestLatch_stageInit1.resolve(sto, dto);
                });
        matsRule.getMatsFactory().terminator(TERMINATOR + "_stageInit2", StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("StageInit2 TERMINATOR MatsTrace:\n" + context.toString());
                    _traceId_stageInit2 = context.getTraceId();
                    matsTestLatch_stageInit2.resolve(sto, dto);
                });
        // :: Terminator for the initiation within the stage executed directly on the MatsFactory.
        matsRule.getMatsFactory().terminator(TERMINATOR + "_stageInit_withMatsFactory", StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("StageInit2 TERMINATOR MatsTrace:\n" + context.toString());
                    _traceId_stageInit_withMatsFactory = context.getTraceId();
                    matsTestLatch_stageInit_withMatsFactory.resolve(sto, dto);
                });
    }

    @Test
    public void doTest() {
        // :: Send message to the single stage service.
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        String randomId = randomId();
        matsRule.getMatsInitiator().initiateUnchecked(
                msg -> msg.traceId(randomId)
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // :: Wait synchronously for all three terminators to finish.
        // "Normal" Terminator from the service call
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());
        Assert.assertEquals(randomId, _traceId);

        // Terminator "stageInit1", for the first initiation within the service's stage
        Result<StateTO, DataTO> result_stageInit1 = matsTestLatch_stageInit1.waitForResult();
        Assert.assertEquals(new StateTO(0, 0), result_stageInit1.getState());
        Assert.assertEquals(new DataTO(Math.E, "xyz"), result_stageInit1.getData());
        Assert.assertEquals(randomId + "|subtraceId1", _traceId_stageInit1);

        // Terminator "stageInit2", for the second initiation within the service's stage
        Result<StateTO, DataTO> result_stageInit2 = matsTestLatch_stageInit2.waitForResult();
        Assert.assertEquals(new StateTO(Integer.MAX_VALUE, Math.PI), result_stageInit2.getState());
        Assert.assertEquals(new DataTO(-Math.E, "abc"), result_stageInit2.getData());
        Assert.assertEquals(randomId + "|subtraceId2", _traceId_stageInit2);

        // Terminator "stageInit_withMatsFactory", for the initiation using MatsFactory within the service's stage
        Result<StateTO, DataTO> result_withMatsFactory = matsTestLatch_stageInit_withMatsFactory.waitForResult();
        Assert.assertEquals(new StateTO(0, 0), result_withMatsFactory.getState());
        Assert.assertEquals(new DataTO(Math.PI, "Endre"), result_withMatsFactory.getData());
        Assert.assertEquals("New TraceId", _traceId_stageInit_withMatsFactory);
    }
}
