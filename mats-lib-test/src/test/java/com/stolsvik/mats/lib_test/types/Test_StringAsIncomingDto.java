package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * An Endpoint can specify String as incoming DTO class, and will thus get the JSON directly - which somewhat resembles
 * a method specifying Object as parameter.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_StringAsIncomingDto extends MatsBasicTest {
    @Before
    public void setupTerminator() {
        // Specifies String as incoming DTO class
        matsRule.getMatsFactory().terminator(TERMINATOR, String.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });
    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsFactory().createInitiator().initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        .send(dto));

        // Wait synchronously for terminator to finish.
        Result<String, StateTO> result = matsTestLatch.waitForResult();
        // Empty State (not provided in invocation)
        Assert.assertEquals(new StateTO(), result.getState());
        // We specified String as incoming DTO, so we'll get the JSON of the request DTO directly.
        Assert.assertEquals("{\"number\":42.0,\"string\":\"TheAnswer\",\"multiplier\":0}", result.getData());
    }
}