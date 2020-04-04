package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests that a service that specifies Object as its Reply type can really return whatever it wants, and that whatever
 * is returned will be serialized as the type it is, not what was specified (i.e. Object), which the receiver will
 * receive correctly. And that if it returns null, the receiver will return null.
 * <p />
 * <b>NOTE: If the ReplyClass==Void.TYPE, the only thing you can answer is 'null'.</b>
 *
 * @see Test_DifferingFromSpecifiedTypes_ForReplyAndIncoming
 * @see Test_ServiceInvokedWithDifferentClass
 *
 * @author Endre Stølsvik 2020-04-03 22:00 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_ReplyClass_Object extends MatsBasicTest {
    @Before
    public void setupService_ReplyClass_Object() {
        matsRule.getMatsFactory().single(SERVICE, Object.class, String.class,
                (context, incoming) -> {
                    if (incoming.equals("String")) {
                        return "Stringeling";
                    }
                    else if (incoming.equals("DataTO")) {
                        return new DataTO(1, "DataTO");
                    }
                    else if (incoming.equals("StateTO")) {
                        return new StateTO(2, Math.PI);
                    }
                    else {
                        return null;
                    }
                });
    }

    @Before
    public void setupTerminator_String() {
        matsRule.getMatsFactory().terminator(TERMINATOR + ".String", StateTO.class, String.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR.String MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                });

    }

    @Before
    public void setupTerminator_DataTO() {
        matsRule.getMatsFactory().terminator(TERMINATOR + ".DataTO", StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR.DataTO MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                });

    }

    @Before
    public void setupTerminator_StateO() {
        matsRule.getMatsFactory().terminator(TERMINATOR + ".StateTO", StateTO.class, StateTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR.StateTO MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                });

    }

    public void test(String replyToTerminator, String serviceRequestedReply, Object expected) {
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR + "." + replyToTerminator, sto)
                        .request(serviceRequestedReply));

        // Wait synchronously for terminator to finish.
        Result<StateTO, Object> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(expected, result.getData());
    }

    @Test
    public void testWithTerminatorString() {
        test("String", "String", "Stringeling");
    }

    @Test
    public void testWithTerminatorDataTO() {
        test("DataTO", "DataTO", new DataTO(1, "DataTO"));
    }

    @Test
    public void testWithTerminatorStateTO() {
        test("StateTO", "StateTO", new StateTO(2, Math.PI));
    }

    @Test
    public void testWithTerminatorString_null() {
        test("String", "null", null);
    }

    @Test
    public void testWithTerminatorDataTO_null() {
        test("DataTO", "null", null);
    }

    @Test
    public void testWithTerminatorStateTO_null() {
        test("StateTO", "null", null);
    }
}
