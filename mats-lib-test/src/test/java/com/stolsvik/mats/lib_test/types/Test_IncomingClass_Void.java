package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests an endpoint specifying incomingClass=Void.TYPE - <b>which results in the incoming message always being
 * null</b>, even if it was sent an actual object.
 *
 * @author Endre Stølsvik 2020-04-03 21:12 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_IncomingClass_Void extends MatsBasicTest {

    @Before
    public void setupTerminator() {
        // Notice how this ignores the incoming message in the Assert inside here.
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, Void.class,
                (context, sto, msg) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());

                    // When accepting Void.TYPE (i.e. void.class), we get null as message, whatever is sent in.
                    Assert.assertNull(msg);

                    matsTestLatch.resolve(sto, new DataTO(sto.number2, "" + sto.number1));
                });
    }

    public void test(Object messageToSend) {
        // To have something to "resolve" with and assert against, we give it additional state.
        StateTO sto = new StateTO(42, Math.E);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        // NOTE: The Terminator will roundly ignore whatever we send it.
                        .send(messageToSend, sto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(new DataTO(sto.number2, "" + sto.number1), result.getData());
        Assert.assertEquals(sto, result.getState());
    }

    @Test
    public void testWithIncomingDataTO() {
        test(new DataTO(42, "TheAnswer"));
    }

    @Test
    public void testWithIncomingString() {
        test("test123");
    }

    @Test
    public void testWithNullAsIncoming() {
        test(null);
    }

}
