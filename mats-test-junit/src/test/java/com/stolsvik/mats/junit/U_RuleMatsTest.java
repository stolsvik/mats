package com.stolsvik.mats.junit;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.stolsvik.mats.MatsInitiator.MatsBackendException;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendException;

/**
 * Simplest possible test case utilizing only {@link Rule_Mats} and {@link Rule_MatsEndpoint}
 * <p>
 * Instead of using a {@link com.stolsvik.mats.util.MatsFuturizer} this test will utilize a {@link Rule_MatsEndpoint} as
 * a terminator and "receive" the reply on this endpoint.
 *
 * @author Kevin Mc Tiernan, 2020-11-10, kmctiernan@gmail.com
 */
public class U_RuleMatsTest {

    @ClassRule
    public static final Rule_Mats __mats = Rule_Mats.createRule();

    @Rule
    public final Rule_MatsEndpoint<String, String> _helloEndpoint =
            Rule_MatsEndpoint.single("HelloEndpoint", String.class, String.class, (ctx, msg) -> "Hello " + msg)
                    .setMatsFactory(__mats.getMatsFactory());

    @Rule
    public final Rule_MatsEndpoint<String, String> _replyEndpoint =
            Rule_MatsEndpoint.single("ReplyEndpoint", String.class, String.class).setMatsFactory(__mats.getMatsFactory());

    @Test
    public void getReplyFromEndpoint() throws MatsMessageSendException, MatsBackendException {
        // :: Send a message to hello endpoint with a specified reply as the specified reply endpoint.
        __mats.getMatsInitiator().initiate(msg -> msg.to("HelloEndpoint")
                .traceId(getClass().getSimpleName() + "[replyFromEndpointTest]")
                .from(getClass().getSimpleName())
                .replyTo("ReplyEndpoint", null)
                .request("World!")
        );

        // :: Wait for the message to reach our replyEndpoint
        String request = _replyEndpoint.waitForRequest();

        // :: Verify
        Assert.assertEquals("Hello World!", request);
    }
}
