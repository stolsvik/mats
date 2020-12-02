package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;

import com.stolsvik.mats.MatsInitiator.MessageReference;
import com.stolsvik.mats.test.junit.Rule_Mats;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestHelp;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Test that the incoming MatsMessageId is the same as we got when sending it.
 * 
 * @author Endre Stølsvik 2019-06-30 22:47 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_MessageReference {
    private static final Logger log = MatsTestHelp.getClassLogger();

    @ClassRule
    public static final Rule_Mats MATS = Rule_Mats.create();

    private static final String TERMINATOR = MatsTestHelp.terminator();

    @BeforeClass
    public static void setupTerminator() {
        MATS.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    MATS.getMatsTestLatch().resolve(context, sto, dto);
                });
    }

    @Test
    public void doTest() {
        StateTO sto = new StateTO(7, 3.14);
        DataTO dto = new DataTO(42, "TheAnswer");
        MessageReference[] msgRef = new MessageReference[1];
        MATS.getMatsInitiator().initiateUnchecked(
                (msg) -> {
                    MessageReference messageReference = msg.traceId(MatsTestHelp.traceId())
                            .from(MatsTestHelp.from("test"))
                            .to(TERMINATOR)
                            .send(dto, sto);
                    msgRef[0] = messageReference;
                });

        log.info("MessageReference.getMatsMessageId() = [" + msgRef[0].getMatsMessageId() + "].");
        Assert.assertNotNull(msgRef[0].getMatsMessageId());

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = MATS.getMatsTestLatch().waitForResult();
        Assert.assertEquals(dto, result.getData());
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(msgRef[0].getMatsMessageId(), result.getContext().getMatsMessageId());
    }

}
