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
 * Illustrates the usage of {@link Rule_Mats}.
 *
 * @author Kevin Mc Tiernan, 2020-11-09, kmctiernan@gmail.com
 */
public class U_RuleMatsWithFuturizerTest {

    @ClassRule
    public static final Rule_Mats MATS = Rule_Mats.createRule();

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
     * Simple test to verify that we actually have a factory and a valid broker.
     */
    @Test
    public void verifyValidMatsFactoryCreated() throws InterruptedException, ExecutionException, TimeoutException {
        // :: Setup - Create an endpoint.
        MATS.getMatsFactory().single("MyEndpoint", String.class, String.class, (ctx, msg) -> "Hello " + msg);

        // :: Act
        String reply = _matsFuturizer.futurizeInteractiveUnreliable("VerifyValidMatsFactory",
                getClass().getSimpleName(),
                "MyEndpoint",
                String.class,
                "World!")
                .thenApply(Reply::getReply)
                .get(10, TimeUnit.SECONDS);

        // :: Verify
        Assert.assertEquals("Hello World!", reply);
    }
}
