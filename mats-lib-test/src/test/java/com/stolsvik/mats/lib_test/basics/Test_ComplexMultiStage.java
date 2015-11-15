package com.stolsvik.mats.lib_test.basics;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.lib_test.AMatsTest;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Sets up a somewhat complex test scenario, testing the basic state keeping and request-reply passing.
 * <p>
 * Sets up these services:
 * <ul>
 * <li>Leaf service: Single stage: Replies directly.
 * <li>Mid service: Two stages: Requests "Leaf" service, then replies.
 * <li>Master service: Three stages: First requests "Mid" service, then requests "Leaf" service, then replies.
 * </ul>
 * A Terminator is also set up, and then the initiator sends a request to "Master", setting replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]
 *     [Master S0, init]
 *         [Mid S0, init]
 *             [Leaf]
 *         [Mid S1, last]
 *     [Master S1]
 *         [Leaf]
 *     [Master S2, last]
 * [Terminator]
 * </pre>
 *
 * @author Endre Stølsvik - 2015-07-31 - http://endre.stolsvik.com
 */
public class Test_ComplexMultiStage extends AMatsTest {
    @Before
    public void setupLeafService() {
        matsRule.getMatsFactory().single(SERVICE + ".Leaf", DataTO.class, DataTO.class, (context, dto) -> {
            return new DataTO(dto.number * 2, dto.string + ":FromLeafService");
        });
    }

    @Before
    public void setupMidMultiStagedService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE + ".Mid", StateTO.class,
                DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(0, 0), sto);
            sto.number1 = 10;
            sto.number2 = Math.PI;
            context.request(SERVICE + ".Leaf", dto);
        });
        ep.lastStage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(10, Math.PI), sto);
            return new DataTO(dto.number * 3, dto.string + ":FromMidService");
        });
    }

    @Before
    public void setupMasterMultiStagedService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE, StateTO.class, DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(0, 0), sto);
            sto.number1 = Integer.MAX_VALUE;
            sto.number2 = Math.E;
            context.request(SERVICE + ".Mid", dto);
        });
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(Integer.MAX_VALUE, Math.E), sto);
            sto.number1 = Integer.MIN_VALUE;
            sto.number2 = Math.E * 2;
            context.request(SERVICE + ".Leaf", dto);
        });
        ep.lastStage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(Integer.MIN_VALUE, Math.E * 2), sto);
            return new DataTO(dto.number * 5, dto.string + ":FromMasterService");
        });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class, (context, dto, sto) -> {
            matsTestLatch.resolve(dto, sto);
        });
    }

    @Test
    public void doTest() throws JMSException, InterruptedException {
        StateTO sto = new StateTO(420, 420.024);
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            msg.traceId(randomId()).from(INITIATOR).to(SERVICE).replyTo(TERMINATOR).request(dto, sto);
        });

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2 * 3 * 2 * 5, dto.string + ":FromLeafService" + ":FromMidService"
                + ":FromLeafService" + ":FromMasterService"), result.getData());
    }
}
