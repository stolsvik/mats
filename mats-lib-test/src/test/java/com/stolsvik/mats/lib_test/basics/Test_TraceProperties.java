package com.stolsvik.mats.lib_test.basics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Tests MatsTrace properties functionality: A single-stage service that reads some MatsTrace properties, and sets one
 * extra, is set up. A Terminator which reads all properties is set up. Then an initiator does a request to the service,
 * setting some MatsTrace properties, and which sets replyTo(Terminator).
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]   - request, adding a String property, and a DataTO property.
 *     [Service] - reply - and reads the two properties (also checking the "read as JSON" functionality), and sets a new property.
 * [Terminator]  - reads all three properties.
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_TraceProperties extends MatsBasicTest {
    private String _stringProp_service;
    private DataTO _objectProp_service;
    private String _objectPropAsString_service;

    private String _stringProp_terminator;
    private DataTO _objectProp_terminator;
    private DataTO _objectPropFromService_terminator;

    private String _stringProp_terminator_initiatedWithinService;
    private DataTO _objectProp_terminator_initiatedWithinService;
    private DataTO _objectPropFromService_terminator_initiatedWithinService;
    private DataTO _objectPropFromService_terminator_initiatedWithinService_setInInitiation;

    private String TERMINATOR_FOR_MESSAGE_INITIATED_WITHIN_SERVICE = TERMINATOR+"-ForMessageInitiatedInService";

    private MatsTestLatch _secondLatch = new MatsTestLatch();

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

                    context.initiate(msg -> {
                        msg.traceId(randomId())
                                .from(INITIATOR)
                                .to(TERMINATOR_FOR_MESSAGE_INITIATED_WITHIN_SERVICE)
                                .setTraceProperty("objectPropInitiatedWithinService", new DataTO(Math.exp(5), "123"))
                                .send(new DataTO(13.14, "qwerty"), new StateTO(42, 420.024));
                    });

                    // Return this service's value to caller.
                    return new DataTO(dto.number * 2, dto.string + ":FromService");
                });
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    // Get the String prop
                    _stringProp_terminator = context.getTraceProperty("stringProp", String.class);
                    // Get the Object prop
                    _objectProp_terminator = context.getTraceProperty("objectProp", DataTO.class);

                    // Get the Object prop set in the Service
                    _objectPropFromService_terminator = context.getTraceProperty("objectPropFromService", DataTO.class);

                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                });

    }

    @Before
    public void setupTerminatorForMessageInitiatedWithinService() {
        matsRule.getMatsFactory().terminator(TERMINATOR_FOR_MESSAGE_INITIATED_WITHIN_SERVICE, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    // Get the String prop
                    _stringProp_terminator_initiatedWithinService = context.getTraceProperty("stringProp", String.class);
                    // Get the Object prop
                    _objectProp_terminator_initiatedWithinService = context.getTraceProperty("objectProp", DataTO.class);

                    // Get the Object prop set in the Service
                    _objectPropFromService_terminator_initiatedWithinService = context.getTraceProperty("objectPropFromService", DataTO.class);

                    // Get the Object prop set in initiation within the Service
                    _objectPropFromService_terminator_initiatedWithinService_setInInitiation = context.getTraceProperty("objectPropInitiatedWithinService", DataTO.class);

                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    _secondLatch.resolve(sto, dto);
                });

    }

    @Test
    public void doTest() {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsInitiator().initiateUnchecked(
                msg -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .setTraceProperty("objectProp", new DataTO(Math.E, "abc"))
                        .setTraceProperty("stringProp", "xyz")
                        .request(dto));

        // ::: Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> primaryResult = matsTestLatch.waitForResult();

        Assert.assertEquals(sto, primaryResult.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), primaryResult.getData());

        // :: Assert all the properties when read from Service
        Assert.assertEquals("xyz", _stringProp_service);
        Assert.assertEquals(new DataTO(Math.E, "abc"), _objectProp_service);
        Assert.assertEquals("{\"number\":2.718281828459045,\"string\":\"abc\",\"multiplier\":0}",
                _objectPropAsString_service);

        // :: Assert all the properties when read from Terminator
        Assert.assertEquals("xyz", _stringProp_terminator);
        Assert.assertEquals(new DataTO(Math.E, "abc"), _objectProp_terminator);
        // .. also the one set inside the Service.
        Assert.assertEquals(new DataTO(Math.PI, "xyz"), _objectPropFromService_terminator);

        // ::: Now get the result for the secondary Terminator, which gets a message initiated from within the service.
        Result<StateTO, DataTO> resultInitiatedWithinService = _secondLatch.waitForResult();
        Assert.assertEquals(new StateTO(42, 420.024), resultInitiatedWithinService.getState());
        Assert.assertEquals(new DataTO(13.14, "qwerty"), resultInitiatedWithinService.getData());

        // :: Assert all the properties when read from second Terminator (for the message initiated within the service)
        Assert.assertEquals("xyz", _stringProp_terminator_initiatedWithinService);
        Assert.assertEquals(new DataTO(Math.E, "abc"), _objectProp_terminator_initiatedWithinService);
        // .. also the one set in the initiation
        Assert.assertEquals(new DataTO(Math.exp(5), "123"), _objectPropFromService_terminator_initiatedWithinService_setInInitiation);
        // .. also the one set inside the Service. before the initiation started.
        Assert.assertEquals(new DataTO(Math.PI, "xyz"), _objectPropFromService_terminator_initiatedWithinService);
    }
}
