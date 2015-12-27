package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests MatsTrace properties functionality: A single-stage service that reads some MatsTrace properties, and sets one
 * extra, is set up. A Terminator which reads all properties is set up. Then an initiator does a request to the service,
 * setting some MatsTrace properties, and which sets replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]   - request, adding a String property, and a DataTO property
 *     [Service] - reply - and reads the two properties (also checking the "read as JSON" functionality), and sets a new property
 * [Terminator]  - reads all three properties
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_MatsTraceProperties extends MatsBasicTest {
    private String _stringProp_service;
    private DataTO _objectProp_service;
    private String _objectPropAsString_service;

    private String _stringProp_terminator;
    private DataTO _objectProp_terminator;
    private DataTO _objectPropFromService_terminator;

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, dto) -> {
                    // Get the String prop
                    _stringProp_service = context.getTraceProperty("stringProp", String.class);
                    // Get the Object prop
                    _objectProp_service = context.getTraceProperty("objectProp", DataTO.class);
                    // Get the Object prop as a String
                    _objectPropAsString_service = context.getTraceProperty("objectProp", String.class);

                    // Add a new Object prop
                    context.setTraceProperty("objectPropFromService", new DataTO(Math.PI, "xyz"));

                    // Return this service's value to caller.
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> {
                    // Get the String prop
                    _stringProp_terminator = context.getTraceProperty("stringProp", String.class);
                    // Get the Object prop
                    _objectProp_terminator = context.getTraceProperty("objectProp", DataTO.class);
                    // Get the Object prop set in the Service
                    _objectPropFromService_terminator = context.getTraceProperty("objectPropFromService", DataTO.class);
                    log.debug("TERMINATOR MatsTrace:\n" + context.getTrace());
                    matsTestLatch.resolve(dto, sto);
                });

    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsFactory().getInitiator(INITIATOR).initiate(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR)
                        .setTraceProperty("objectProp", new DataTO(Math.E, "abc"))
                        .setTraceProperty("stringProp", "xyz")
                        .request(dto, sto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());

        // :: Assert all the properties when read from Service
        Assert.assertEquals("xyz", _stringProp_service);
        Assert.assertEquals(new DataTO(Math.E, "abc"), _objectProp_service);
        Assert.assertEquals("{\"number\":2.718281828459045,\"string\":\"abc\"}", _objectPropAsString_service);

        // :: Assert all the properties when read from Terminator, then also including the one set in Service
        Assert.assertEquals("xyz", _stringProp_terminator);
        Assert.assertEquals(new DataTO(Math.E, "abc"), _objectProp_terminator);
        Assert.assertEquals(new DataTO(Math.PI, "xyz"), _objectPropFromService_terminator);
    }
}
