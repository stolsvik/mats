package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests an endpoint replying with null - where the Reply DTO is specified as either Void, Object or DataTO.
 *
 * @author Endre Stølsvik 2020-04-03 21:47 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_ReplyClass_Void_Object_DataTO_ReplyWithNull extends MatsBasicTest {
    @Before
    public void setupService_ReplyClass_Void() {
        matsRule.getMatsFactory().single(SERVICE + ".ReplyClass_Void", Void.class, DataTO.class,
                (context, dto) -> null);
    }

    @Before
    public void setupService_ReplyClass_Object() {
        matsRule.getMatsFactory().single(SERVICE + ".ReplyClass_Object", Object.class, DataTO.class,
                (context, dto) -> null);
    }

    @Before
    public void setupService_ReplyClass_DataTO() {
        matsRule.getMatsFactory().single(SERVICE + ".ReplyClass_DataTO", DataTO.class, DataTO.class,
                (context, dto) -> null);
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                });

    }

    public void test(String toService) {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE + "."+toService)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertNull(result.getData());
    }

    @Test
    public void testWithService_ReplyClass_DataTO() {
        test("ReplyClass_DataTO");
    }

    @Test
    public void testWithService_ReplyClass_Void() {
        test("ReplyClass_Void");
    }

    @Test
    public void testWithService_ReplyClass_Object() {
        test("ReplyClass_Object");
    }

}
