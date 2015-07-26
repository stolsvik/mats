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
            matsTestLatch.resolve(sto, dto);
        });
    }

    @Test
    public void doTest() throws JMSException, InterruptedException {
        DataTO dto = new DataTO(420, "DataAnswer");
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            msg.from(INITIATOR).to(TERMINATOR).invoke(dto);
        });

        // Wait synchronously - due to test scenario - for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(dto, result.getData());
    }
}
