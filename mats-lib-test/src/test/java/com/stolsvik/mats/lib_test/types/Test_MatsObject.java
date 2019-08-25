package com.stolsvik.mats.lib_test.types;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint.MatsObject;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the {@link MatsObject} special snacks.
 *
 * @author Endre Stølsvik 2019-08-25 00:49 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_MatsObject extends MatsBasicTest {
    private volatile IllegalArgumentException _exception;

    @Before
    public void setupTerminator1() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, MatsObject.class,
                (context, sto, matsObject) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    try {
                        matsObject.toClass(List.class);
                    }
                    catch (IllegalArgumentException iaE) {
                        _exception = iaE;
                    }
                    DataTO dto = matsObject.toClass(DataTO.class);
                    matsTestLatch.resolve(sto, dto);
                });
    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        .send(dto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(dto, result.getData());
        Assert.assertEquals(IllegalArgumentException.class, _exception.getClass());
        try {
            Thread.sleep(50);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
