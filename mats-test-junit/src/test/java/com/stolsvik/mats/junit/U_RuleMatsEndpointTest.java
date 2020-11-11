package com.stolsvik.mats.junit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.stolsvik.mats.util.MatsFuturizer;
import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Illustrate some of the features of {@link Rule_MatsEndpoint}.
 * <p>
 * Also illustrates the usage of {@link Rule_MatsEndpoint} in combination with {@link Rule_Mats}.
 *
 * @author Kevin Mc Tiernan, 2020-11-09, kmctiernan@gmail.com
 */
public class U_RuleMatsEndpointTest {

    public static final String HELLO_ENDPOINT_ID = "HelloEndpoint";

    @ClassRule
    public static final Rule_Mats MATS = Rule_Mats.createRule();

    @Rule
    public Rule_MatsEndpoint<String, String> _helloEndpoint = Rule_MatsEndpoint.single(HELLO_ENDPOINT_ID, String.class,
            String.class)
            .setMatsFactory(MATS.getMatsFactory());

    private MatsFuturizer _matsFuturizer;

    @Before
    public void beforeEach() {
        _matsFuturizer = MatsFuturizer.createMatsFuturizer(MATS.getMatsFactory(), this.getClass().getSimpleName());
    }

    @After
    public void afterEach() {
        _matsFuturizer.close();
        MATS.removeAllEndpoints();
    }

    /**
     * Shows that when no processor is defined, an endpoint will not produce a reply.
     */
    @Test
    public void noProcessorDefined() {
        Throwable throwExpected = null;

        // :: Send a message to the endpoint - This will timeout, thus wrap it in a try-catch.
        try {
            _matsFuturizer.futurizeInteractiveUnreliable(getClass().getSimpleName() + "|noProcessorDefined",
                    getClass().getSimpleName(),
                    HELLO_ENDPOINT_ID,
                    String.class,
                    "World")
                    .thenApply(Reply::getReply)
                    .get(5, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            throwExpected = e;
        }
        catch (InterruptedException | ExecutionException e) {
            throw new AssertionError("Expected timeout exception, not this!", e);
        }

        // ----- At this point the above block has timed out. Now we need to verify that the endpoint actually got the
        // message and that the exception throw above was indeed a TimeoutException.
        String incomingMsgToTheEndpoint = _helloEndpoint.waitForRequests(1).get(0);

        Assert.assertNotNull(throwExpected);
        Assert.assertEquals(TimeoutException.class, throwExpected.getClass());

        Assert.assertEquals("World", incomingMsgToTheEndpoint);
    }

    /**
     * Executes two calls to the same "mock" endpoint where the processor logic of the endpoint is changed mid test to
     * verify that this feature works as expected.
     */
    @Test
    public void changeProcessorMidTestTest() throws InterruptedException, ExecutionException, TimeoutException {
        String expectedReturn = "Hello World!";
        String secondExpectedReturn = "Hello Wonderful World!";

        // :: First Setup
        _helloEndpoint.setProcessLambda((ctx, msg) -> msg + " World!");

        // :: First Act
        String firstReply = _matsFuturizer.futurizeInteractiveUnreliable(
                getClass().getSimpleName() + "_changeProcessorMidTestTest",
                        getClass().getSimpleName(),
                        HELLO_ENDPOINT_ID,
                        String.class,
                        "Hello")
                        .thenApply(Reply::getReply)
                        .get(10, TimeUnit.SECONDS);

        // :: First Verify
        Assert.assertEquals(expectedReturn, firstReply);

        // :: Second Setup
        _helloEndpoint.setProcessLambda((ctx, msg) -> msg + " Wonderful World!");

        // :: Second Act
        String secondReply = _matsFuturizer.futurizeInteractiveUnreliable(
                getClass().getSimpleName() + "_changeProcessorMidTestTest",
                getClass().getSimpleName(),
                HELLO_ENDPOINT_ID,
                String.class,
                "Hello")
                .thenApply(Reply::getReply)
                .get(10, TimeUnit.SECONDS);

        // :: Final verify
        Assert.assertEquals(secondExpectedReturn, secondReply);
    }
}
