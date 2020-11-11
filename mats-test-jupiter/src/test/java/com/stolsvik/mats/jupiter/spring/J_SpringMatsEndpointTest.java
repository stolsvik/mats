package com.stolsvik.mats.jupiter.spring;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.stolsvik.mats.jupiter.Extension_MatsEndpoint;
import com.stolsvik.mats.util.MatsFuturizer;
import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Illustrates that {@link Extension_MatsEndpoint} is autowired by Spring when using the test execution listener
 * provided by this library.
 *
 * @author Kevin Mc Tiernan, 2020-11-10, kmctiernan@gmail.com
 */
public class J_SpringMatsEndpointTest extends SpringContextTest {

    @RegisterExtension // Extension autowired by Spring
    public Extension_MatsEndpoint<String, String> _hello =
            Extension_MatsEndpoint.single("Hello", String.class, String.class, (ctx, msg) -> "Hello " + msg);

    @Inject
    private MatsFuturizer _matsFuturizer;

    @Test
    public void verifyEndpointExists() throws InterruptedException, ExecutionException, TimeoutException {
        // :: Act
        String reply = _matsFuturizer.futurizeInteractiveUnreliable("VerifyValidMatsFactory",
                getClass().getSimpleName(),
                "Hello",
                String.class,
                "World!")
                .thenApply(Reply::getReply)
                .get(10, TimeUnit.SECONDS);

        // :: Verify
        Assertions.assertEquals("Hello World!", reply);
    }
}
