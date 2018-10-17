package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests that a Service can be invoked with a different class than it specifies - the "contract" is really the
 * underlying JSON.
 *
 * @author Endre Stølsvik - 2016-06-20 - http://endre.stolsvik.com
 */
public class Test_ServiceInvokedWithDifferentClass extends MatsBasicTest {

    /**
     * The service expects a completely unrelated class compared to what we send it.
     */
    public static class DifferentDTO {
        public double number;
        public String string;
        public String otherString;

        public DifferentDTO() {
            // For Jackson JSON-lib which needs default constructor.
        }

        public DifferentDTO(double number, String string, String otherString) {
            this.number = number;
            this.string = string;
            this.otherString = otherString;
        }

        @Override
        public String toString() {
            return "DifferentDTO [number=" + number + ", string=" + string + ", otherString=" + otherString + "]";
        }
    }

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DifferentDTO.class, DifferentDTO.class,
                (context, dto) -> new DifferentDTO(dto.number * 2,
                        dto.string + ":TimesTwo",
                        dto.string + ":FromService"));
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, SubDataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(dto, sto);
                });

    }

    @Test
    public void doTest() {
        DifferentDTO dto = new DifferentDTO(16, "Sixteen", "ThatOtherThing");
        StateTO sto = new StateTO(256, 5.12);
        matsRule.getMatsFactory().createInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for terminator to finish.
        Result<SubDataTO, StateTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        SubDataTO data = result.getData();

        // :: Assert each field of the reply separately
        // .number should be there
        Assert.assertEquals(dto.number * 2, data.number, 0.1d);
        // .string should be there
        Assert.assertEquals(dto.string + ":TimesTwo", data.string);
        // .stringSet should NOT be present, as the return value does not contain it.
        Assert.assertEquals(null, data.stringSet);
    }
}
