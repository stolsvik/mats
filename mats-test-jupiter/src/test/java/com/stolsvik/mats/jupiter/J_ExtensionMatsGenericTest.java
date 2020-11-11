package com.stolsvik.mats.jupiter;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util.MatsFuturizer;
import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Similar to {@link J_ExtensionMatsTest} illustrating the usage of {@link Extension_Mats}, this class illustrates the
 * usage of {@link Extension_MatsGeneric}.
 *
 * @author Kevin Mc Tiernan, 2020-11-09, kmctiernan@gmail.com
 */
public class J_ExtensionMatsGenericTest {

    @RegisterExtension
    public static final Extension_MatsGeneric<String> __mats =
            new Extension_MatsGeneric<>(new MatsSerializer_DefaultJson());

    private MatsFuturizer _matsFuturizer;

    @BeforeEach
    public void beforeEach() {
        _matsFuturizer = MatsFuturizer.createMatsFuturizer(__mats.getMatsFactory(), this.getClass().getSimpleName());
    }

    @AfterEach
    public void afterEach() {
        _matsFuturizer.close();
    }

    /**
     * Simple test to verify that we actually have a factory and a valid broker.
     */
    @Test
    public void verifyValidMatsFactoryCreated() throws InterruptedException, ExecutionException, TimeoutException {
        // :: Setup
        __mats.getMatsFactory().single("MyEndpoint", String.class, String.class, (ctx, msg) -> "Hello " + msg);

        // :: Act
        String reply = _matsFuturizer.futurizeInteractiveUnreliable("VerifyValidMatsFactory",
                getClass().getSimpleName(),
                "MyEndpoint",
                String.class,
                "World!")
                .thenApply(Reply::getReply)
                .get(10, TimeUnit.SECONDS);

        // :: Verify
        Assertions.assertEquals("Hello World!", reply);
    }
}
