package com.stolsvik.mats.lib_test.types;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests that a Service can be invoked with a different class than it specifies - the "contract" is really the
 * underlying JSON, and any coinciding fields between the incoming message and the receiving Endpoints will be set (i.e.
 * the resulting Incoming object's set fields will be the intersection between the Reply class and the expected Incoming
 * class). This is barring type clashes - if the Reply specifies 'camels' as an int, while the receiving
 * endpoint/terminator expects 'camels' to be a List&lt;Camel&gt;, then expect a DLQ.
 *
 * @see Test_DifferingFromSpecifiedTypes_ForReplyAndIncoming
 * @see Test_ReplyClass_Object
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
                    matsTestLatch.resolve(sto, dto);
                });

    }

    @Test
    public void doTest() {
        DifferentDTO dto = new DifferentDTO(16, "Sixteen", "ThatOtherThing");
        StateTO sto = new StateTO(256, 5.12);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, SubDataTO> result = matsTestLatch.waitForResult();
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
