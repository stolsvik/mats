package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests the send-along bytes and Strings properties with the message - close to copy of the {@link Test_MutilStageNext}
 * except testing the properties too. Test initiation, next, and reply.
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]             - init request (adds props)
 *     [Service S0 - init] - next    (retrieves props, modifies them and adds to next)
 *     [Service S1 - last] - reply   (retrieves props, modifies them and adds to next)
 * [Terminator]
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_PropertiesBytesAndStrings extends MatsBasicTest {
    @Before
    public void setupMultiStageService() {
        MatsEndpoint<StateTO, DataTO> ep = matsRule.getMatsFactory().staged(SERVICE, StateTO.class, DataTO.class);
        ep.stage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(0, 0), sto);
            sto.number1 = Integer.MAX_VALUE;
            sto.number2 = Math.E;

            byte[] bytes = context.getBytes("bytes");
            String string = context.getString("string");
            bytes[5] = (byte) (bytes[5] * 2);
            context.addBytes("bytes", bytes);
            context.addString("string", string + ":InitialStage");

            context.next(new DataTO(dto.number * 2, dto.string + ":InitialStage"));
        });
        ep.lastStage(DataTO.class, (context, dto, sto) -> {
            Assert.assertEquals(new StateTO(Integer.MAX_VALUE, Math.E), sto);

            byte[] bytes = context.getBytes("bytes");
            String string = context.getString("string");
            bytes[5] = (byte) (bytes[5] * 3);
            context.addBytes("bytes", bytes);
            context.addString("string", string + ":ReplyStage");

            return new DataTO(dto.number * 3, dto.string + ":ReplyStage");
        });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    _bytes = context.getBytes("bytes");
                    _string = context.getString("string");
                    matsTestLatch.resolve(dto, sto);
                });
    }

    private byte[] _bytes;
    private String _string;

    @Test
    public void doTest() {
        StateTO sto = new StateTO(420, 420.024);
        DataTO dto = new DataTO(42, "TheAnswer");
        byte[] bytes = new byte[] { 0, 1, -1, 127, -128, 11 };
        String string = "TestString";
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR)
                        .addBytes("bytes", bytes)
                        .addString("string", string)
                        .request(dto, sto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2 * 3, dto.string + ":InitialStage" + ":ReplyStage"),
                result.getData());

        bytes[5] = (byte) (bytes[5] * 2 * 3);
        Assert.assertArrayEquals(bytes, _bytes);
        Assert.assertEquals(string + ":InitialStage" + ":ReplyStage", _string);
    }
}
