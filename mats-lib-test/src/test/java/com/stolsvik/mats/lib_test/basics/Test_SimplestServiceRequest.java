package com.stolsvik.mats.lib_test.basics;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.AMatsTest;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the simplest request functionality: A single-stage service is set up. A Terminator is set up. Then an initiator
 * does a request to the service, setting replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]
 *     [Service]
 * [Terminator]
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_SimplestServiceRequest extends AMatsTest {
    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class, (context, dto) -> {
            return new DataTO(dto.number * 2, dto.string + ":FromService");
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
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate((msg) -> {
            msg.traceId(randomId()).from(INITIATOR).to(SERVICE).replyTo(TERMINATOR).request(dto, sto);
        });

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());
    }
}
