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
            // Assert.assertEquals(42, sto.number);
            // Assert.assertEquals("xyz", sto.string);
            matsLatch.resolve(sto, dto);
        });
    }

    @Test
    public void doTest() throws JMSException, InterruptedException {
        StateTO sto = new StateTO(42, "TheAnswer");
        DataTO dto = new DataTO(420, "AnotherAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            msg.to(TERMINATOR).invoke(dto);
        });
        Result<StateTO, DataTO> result = matsLatch.waitForResult();
        // Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(dto, result.getData());
    }
}
