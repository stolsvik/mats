package com.stolsvik.mats.junit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util.MatsFuturizer;
import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Similar to {@link U_RuleMatsWithFuturizerTest} illustrating the usage of {@link Rule_Mats}, this class illustrates the usage of
 * {@link Rule_MatsGeneric}.
 *
 * @author Kevin Mc Tiernan, 2020-11-09, kmctiernan@gmail.com
 */
public class U_RuleMatsGenericTest {

    @ClassRule
    public static Rule_MatsGeneric<String> mats = new Rule_MatsGeneric<>(new MatsSerializer_DefaultJson());

    private MatsFuturizer _matsFuturizer;

    @Before
    public void beforeEach() {
        _matsFuturizer = MatsFuturizer.createMatsFuturizer(mats.getMatsFactory(), this.getClass().getSimpleName());
    }

    @After
    public void afterEach() {
        _matsFuturizer.close();
        mats.removeAllEndpoints();
    }

    /**
     * Simple test to verify that we actually have a factory and a valid broker.
     */
    @Test
    public void verifyValidMatsFactoryCreated() throws InterruptedException, ExecutionException, TimeoutException {
        // :: Setup
        mats.getMatsFactory().single("MyEndpoint", String.class, String.class, (ctx, msg) -> "Hello " + msg);

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
