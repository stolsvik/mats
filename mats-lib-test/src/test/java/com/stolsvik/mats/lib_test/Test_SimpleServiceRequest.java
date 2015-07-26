package com.stolsvik.mats.lib_test;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.test.MatsTestLatch.Result;

public class Test_SimpleServiceRequest extends AMatsTest {
    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class, (context, dto) -> {
            return new DataTO(dto.number * 2, dto.string + ":FromService");
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
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());
    }
}
