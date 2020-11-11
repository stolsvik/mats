package com.stolsvik.mats.junit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Illustrate further usage of {@link Rule_MatsEndpoint}.
 *
 * @author Kevin Mc Tiernan, 2020-11-11, kmctiernan@gmail.com
 */
public class U_RuleMatsEndpointComplexTest extends U_AbstractTestBase {

    /** Imagine this as another micro service in your system which this multistage communicates with. */
    private static final String EXTERNAL_ENDPOINT = "ExternalService.ExternalHello";
    /** Imagine this as an internal endpoint, but we don't want to bring up the class which contains it. */
    private static final String OTHER_INTERNAL_ENDPOINT = "OtherInternal.OtherHello";

    private static final String INTERNAL_RESPONSE = "InternalResponse";
    private static final String EXTERNAL_RESPONSE = "ExternalResponse";

    @Rule // Mock external endpoint
    public Rule_MatsEndpoint<String, String> _external =
            Rule_MatsEndpoint.single(EXTERNAL_ENDPOINT, String.class, String.class, (ctx, msg) -> EXTERNAL_RESPONSE)
                    .setMatsFactory(__mats.getMatsFactory());

    @Rule // Mock internal endpoint
    public Rule_MatsEndpoint<String, String> _internal = Rule_MatsEndpoint
            .single(OTHER_INTERNAL_ENDPOINT, String.class, String.class, (ctx, msg) -> INTERNAL_RESPONSE)
            .setMatsFactory(__mats.getMatsFactory());

    private static final String REQUEST_INTERNAL = "RequestInternalEndpoint";
    private static final String REQUEST_EXTERNAL = "RequestExternalEndpoint";

    @Before
    public void setupMultiStage() {
        // :: Setup up our multi stage.
        MatsEndpoint<String, MultiStageSTO> multiStage =
                __mats.getMatsFactory().staged("MultiStage", String.class, MultiStageSTO.class);
        //:: Receive the initial request, store it and call the internal mock.
        multiStage.stage(String.class, (ctx, state, msg) -> {
            // :: Store the incoming message as the initialRequest.
            state.initialRequest = msg;
            // :: Call the other internal endpoint mock
            ctx.request(OTHER_INTERNAL_ENDPOINT, REQUEST_INTERNAL);
        });
        // :: Receive the internal mock response, store it and query the external mock endpoint.
        multiStage.stage(String.class, (ctx, state, msg) -> {
            // :: Store the response of the internal endpoint.
            state.internalResponse = msg;
            // :: Query the external endpoint.
            ctx.request(EXTERNAL_ENDPOINT, REQUEST_EXTERNAL);
        });
        // :: Receive the external mock response, store it and respond to the initial request.
        multiStage.lastStage(String.class, (ctx, state, msg) -> {
            // :: Store the external response
            state.externalResponse = msg;
            // :: Reply
            return state.initialRequest + "-" + state.internalResponse + "-" + msg;
        });
    }

    @Test
    public void multiStageWithMockEndpoints() throws InterruptedException, ExecutionException, TimeoutException {
        String reply = _matsFuturizer.futurizeInteractiveUnreliable(
                getClass().getSimpleName() + "[multiStageTest]",
                getClass().getSimpleName(),
                "MultiStage",
                String.class,
                "Request")
                .thenApply(Reply::getReply)
                .get(3, TimeUnit.SECONDS);

        // :: Verify
        Assert.assertEquals("Request-InternalResponse-ExternalResponse", reply);

        String requestInternal = _internal.waitForRequest();
        String requestExternal = _external.waitForRequest();

        Assert.assertEquals(REQUEST_INTERNAL, requestInternal);
        Assert.assertEquals(REQUEST_EXTERNAL, requestExternal);
    }

    /** MultiStage state class. */
    public static class MultiStageSTO {
        public String initialRequest;
        public String externalResponse;
        public String internalResponse;
    }
}
