package com.stolsvik.mats.lib_test;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.test.MatsTestLatch.Result;

public class Test_MultiStage extends AMatsTest {
    @Before
    public void setupLeafService() {
        matsRule.getMatsFactory().single(SERVICE + ".Leaf", DataTO.class, DataTO.class, (context, dto) -> {
            return new DataTO(dto.number * 2, dto.string + ":FromLeafService");
        });
    }

    @Before
    public void setupMultiStageService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE, StateTO.class, DataTO.class);
        ep.stage(DataTO.class, (context, sto, dto) -> {
            log.info("State object: " + sto);
            log.info("Data object: " + dto);
            Assert.assertEquals(new StateTO(0, null), sto);
            sto.number = 1;
            sto.string = "A";
            context.request(SERVICE + ".Leaf", dto);
        });
        ep.stage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(1, "A"), sto);
            sto.number = 2;
            sto.string = "B";
            context.request(SERVICE + ".Leaf", dto);
        });
        ep.lastStage(DataTO.class, (context, sto, dto) -> {
            Assert.assertEquals(new StateTO(2, "B"), sto);
            return dto;
        });

    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class, (context, sto, dto) -> {
            matsTestLatch.resolve(sto, dto);
        });
    }

    @Test
    public void doTest() throws JMSException, InterruptedException {
        StateTO sto = new StateTO(42, "StateAnswer");
        DataTO dto = new DataTO(420, "DataAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            msg.from(INITIATOR).to(SERVICE).replyTo(TERMINATOR).request(sto, dto);
        });

        // Wait synchronously - due to test scenario - for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 4, dto.string + ":FromLeafService" + ":FromLeafService"),
                result.getData());
    }

}
