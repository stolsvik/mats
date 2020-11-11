package com.stolsvik.mats.jupiter;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Illustrates the usage of a common test base class which test classes can extend.
 *
 * @author Kevin Mc Tiernan, 2020-11-10, kmctiernan@gmail.com
 */
public class J_UsingTestBaseTest extends J_AbstractTestBase {
    private static final String HELLO_ENDPOINT_ID = "HelloEndpoint";
    private static final String HELLO_RESPONSE = "Hello ";

    @RegisterExtension
    public Extension_MatsEndpoint<String, String> _hello =
            Extension_MatsEndpoint
                    .single(HELLO_ENDPOINT_ID, String.class, String.class, (ctx, msg) -> HELLO_RESPONSE + msg)
                    .setMatsFactory(__mats.getMatsFactory());

    @Test
    public void helloWorld() throws InterruptedException, ExecutionException, TimeoutException {
        // Setup
        String request = "World!";
        String expectedResponse = HELLO_RESPONSE + request;

        // Act
        String response = _matsFuturizer.futurizeInteractiveUnreliable(getClass().getSimpleName() + "[helloWorld]",
                getClass().getSimpleName(),
                HELLO_ENDPOINT_ID,
                String.class,
                request)
                .thenApply(Reply::getReply)
                .get(5, TimeUnit.SECONDS);

        // Verify
        Assertions.assertNotNull(response);
        Assertions.assertEquals(expectedResponse, response);
    }
}
