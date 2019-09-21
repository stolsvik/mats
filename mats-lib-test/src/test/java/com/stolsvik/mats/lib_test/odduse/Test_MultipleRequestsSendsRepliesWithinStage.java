package com.stolsvik.mats.lib_test.odduse;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.stolsvik.mats.MatsInitiator.KeepTrace;
import com.stolsvik.mats.serial.MatsTrace.KeepMatsTrace;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests that it is possible to do multiple context.request, context.send and context.reply within a stage.
 *
 * @author Endre Stølsvik 2019-05-29 18:05 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_MultipleRequestsSendsRepliesWithinStage extends MatsBasicTest {
    private AtomicInteger _serviceInvocationCount = new AtomicInteger(0);
    private AtomicInteger _serviceStage1InvocationCount = new AtomicInteger(0);
    private AtomicInteger _serviceStage2InvocationCount = new AtomicInteger(0);

    @Before
    public void setupTwoStageService() {
        MatsEndpoint<DataTO, StateTO> ep = matsRule.getMatsFactory().staged(SERVICE, DataTO.class, StateTO.class);
        ep.stage(DataTO.class, (context, sto, dto) -> {
            // Should only be invoked once.
            _serviceInvocationCount.incrementAndGet();
            Assert.assertEquals(new StateTO(0, 0), sto);
            sto.number1 = 10;
            sto.number2 = Math.PI;
            // :: Perform /two/ requests to Leaf service (thus replies twice)
            context.request(SERVICE + ".Leaf", new DataTO(dto.number, dto.string + ":Request#1"));
            context.request(SERVICE + ".Leaf", new DataTO(dto.number, dto.string + ":Request#2"));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            // This should now be invoked twice.
            Assert.assertEquals(new StateTO(10, Math.PI), sto);
            _serviceStage1InvocationCount.incrementAndGet();
            sto.number1 = -10;
            sto.number2 = Math.E;
            // :: Send two messages directly to TERMINATOR
            context.initiate((msg) -> msg.to(TERMINATOR)
                    .send(new DataTO(dto.number, dto.string + ":Send#1")));
            context.initiate((msg) -> msg.to(TERMINATOR)
                    .send(new DataTO(dto.number, dto.string + ":Send#2")));
            // :: Pass on /twice/ to next stage (thus 4 times to next stage, since this stage is replied to twice).
            context.next(new DataTO(dto.number, dto.string + ":Next#1"));
            context.next(new DataTO(dto.number, dto.string + ":Next#2"));
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            // This should now be invoked four times (see above on "next" calls).
            _serviceStage2InvocationCount.incrementAndGet();
            Assert.assertEquals(new StateTO(-10, Math.E), sto);
            context.reply(new DataTO(dto.number, dto.string + ":Reply#1"));
            context.reply(new DataTO(dto.number, dto.string + ":Reply#2"));
        });
        ep.finishSetup();
    }

    private AtomicInteger _leafInvocationCount = new AtomicInteger(0);

    @Before
    public void setupLeafService() {
        matsRule.getMatsFactory().single(SERVICE + ".Leaf", DataTO.class, DataTO.class,
                (context, dto) -> {
                    // Should be invoked twice.
                    _leafInvocationCount.incrementAndGet();
                    return new DataTO(dto.number * 2, dto.string + ":FromLeafService");
                });
    }

    private AtomicInteger _terminatorInvocationCount = new AtomicInteger(0);

    private Map<String, String> answers = new ConcurrentHashMap<>();

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    // Should be invoked 12 times, 8 for the request-reply stages + 4 from the sends in middle stage.
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    log.debug("TERMINATOR Incoming Message: " + dto);
                    // Store the answer.
                    answers.put(dto.string, "");
                    int count = _terminatorInvocationCount.incrementAndGet();
                    if (count == 12) {
                        matsTestLatch.resolve(sto, dto);
                    }
                });

    }

    @Test
    public void doTestFull() {
        // 1780 bytes @ Terminator
        doTest(KeepTrace.FULL);
    }

    @Test
    public void doTestCompact() {
        // 1331 bytes @ Terminator
        doTest(KeepTrace.COMPACT);
    }

    @Test
    public void doTestMinimal() {
        // 605 bytes @ Terminator
        doTest(KeepTrace.MINIMAL);
    }

    private void doTest(KeepTrace keepTrace) {
        DataTO dto = new DataTO(1, "Initiator");
        StateTO sto = new StateTO(20, -20);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .keepTrace(keepTrace)
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for terminator to finish (all 8 answers).
        matsTestLatch.waitForResult();

        // ----- All finished

        // :: Assert invocation counts
        Assert.assertEquals(1, _serviceInvocationCount.get());
        Assert.assertEquals(2, _leafInvocationCount.get());
        Assert.assertEquals(2, _serviceStage1InvocationCount.get());
        Assert.assertEquals(4, _serviceStage2InvocationCount.get());
        Assert.assertEquals(12, _terminatorInvocationCount.get());

        // :: These are the 12 replies that the Terminator should see.
        // Effectively a binary permutation of 3x[1|2] + the 2x2 "Send" messages.
        SortedSet<String> expected = new TreeSet<>();
        expected.add("Initiator:Request#1:FromLeafService:Next#1:Reply#1");
        expected.add("Initiator:Request#1:FromLeafService:Next#1:Reply#2");
        expected.add("Initiator:Request#1:FromLeafService:Next#2:Reply#1");
        expected.add("Initiator:Request#1:FromLeafService:Next#2:Reply#2");
        expected.add("Initiator:Request#1:FromLeafService:Send#1");
        expected.add("Initiator:Request#1:FromLeafService:Send#2");
        expected.add("Initiator:Request#2:FromLeafService:Next#1:Reply#1");
        expected.add("Initiator:Request#2:FromLeafService:Next#1:Reply#2");
        expected.add("Initiator:Request#2:FromLeafService:Next#2:Reply#1");
        expected.add("Initiator:Request#2:FromLeafService:Next#2:Reply#2");
        expected.add("Initiator:Request#2:FromLeafService:Send#1");
        expected.add("Initiator:Request#2:FromLeafService:Send#2");

        Assert.assertEquals(expected, new TreeSet<>(answers.keySet()));
    }
}
