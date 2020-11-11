package com.stolsvik.mats.junit.spring;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.stolsvik.mats.junit.Rule_MatsEndpoint;
import com.stolsvik.mats.util.MatsFuturizer;
import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Illustrates that {@link Rule_MatsEndpoint} is autowired by Spring when using the test execution listener
 * provided by this library.
 *
 * @author Kevin Mc Tiernan, 2020-11-10, kmctiernan@gmail.com
 */
public class U_SpringMatsEndpointTest extends AbstractSpringTest {

    @Rule // Rule autowired by Spring
    public Rule_MatsEndpoint<String, String> _hello =
            Rule_MatsEndpoint.single("Hello", String.class, String.class, (ctx, msg) -> "Hello " + msg);

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
        Assert.assertEquals("Hello World!", reply);
    }
}
