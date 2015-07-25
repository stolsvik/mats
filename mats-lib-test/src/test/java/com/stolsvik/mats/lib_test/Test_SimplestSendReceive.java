package com.stolsvik.mats.lib_test;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.test.MatsTestLatch.Result;

public class Test_SimplestSendReceive extends AMatsTest {

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class, (context, sto, dto) -> {
            matsLatch.resolve(sto, dto);
        });
    }

    @Test
    public void doTest() throws JMSException, InterruptedException {
        StateTO sto = new StateTO(42, "StateAnswer");
        DataTO dto = new DataTO(420, "DataAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            msg.from(INITIATOR).to(TERMINATOR).invoke(sto, dto);
        });
        Result<StateTO, DataTO> result = matsLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(dto, result.getData());
    }
}
