package com.stolsvik.mats.util;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.util.MatsFuturizer.Reply;

/**
 * Test for deserialization failure and that this throws an exception to the threads waiting for the Future.
 *
 * @author Hallvard Nygård, 2020, hallvard.nygard@gmail.com
 * @author Kevin Mc Tiernan, 2020, kmctiernan@gmail.com
 */
public class Test_MatsFuturizer_FailedSerializationFuture extends MatsBasicTest {

    @Before
    public void setupServiceEndpoint() {
        matsRule.getMatsFactory().single(SERVICE, DtoWeSend.class, String.class,
                (context, incomingMsg) -> new DtoWeSend(incomingMsg));
    }

    /**
     * Verifies that the deserialization failure is propagated to the get invoker.
     * <p>
     * In this test there is no {@link CompletableFuture#exceptionally(Function)} handling, a get is executed on the
     * future and we assert that the resulting exception is the one we expect.
     */
    @Test(timeout = 5000)
    public void futureGet() {
        MatsFuturizer futurizer = MatsFuturizer.createMatsFuturizer(matsRule.getMatsFactory(), "TestFuturizer");
        String traceId = UUID.randomUUID().toString();

        CompletableFuture<Reply<DtoWeExpect>> future = futurizer.futurizeInteractiveUnreliable(traceId,
                "futureGet",
                SERVICE,
                DtoWeExpect.class, "NOK");
        try {
            Reply<DtoWeExpect> reply = future.get();
            Assert.fail("We should not get a response. Currency was " + reply.getReply().currency);
        }
        catch (Throwable e) {
            log.info("Got the exception. Hoping it's the right one. Logging stacktrace just in case.", e);
            Assert.assertEquals("Could not deserialize the data contained in MatsObject to class"
                    + " [com.stolsvik.mats.util.Test_MatsFuturizer_FailedSerializationFuture$DtoWeExpect].", e.getCause().getMessage());
        }
    }

    /**
     * Verifies that the deserialization failure is propagated and can be handled within
     * {@link CompletableFuture#exceptionally(Function)}.
     */
    @Test(timeout = 5000)
    public void futureThenApply_andExceptionally_Common()
            throws InterruptedException, ExecutionException {
        MatsFuturizer futurizer = MatsFuturizer.createMatsFuturizer(matsRule.getMatsFactory(), "TestFuturizer");
        String traceId = UUID.randomUUID().toString();

        DtoWeExpect r = futurizer.futurizeInteractiveUnreliable(
                traceId,
                "futureGet",
                SERVICE,
                DtoWeExpect.class,
                "NOK")
                .thenApply(Reply::getReply)
                .exceptionally(e -> {
                    log.info("Got the exception. Hoping it's the right one. Logging stacktrace just in case.", e);
                    Assert.assertEquals("Could not deserialize the data contained in MatsObject to class "
                                    + "[com.stolsvik.mats.util.Test_MatsFuturizer_FailedSerializationFuture$DtoWeExpect].",
                            e.getCause().getMessage());
                    return new DtoWeExpect("ExceptionallyTest");
                })
                // Make it wait
                .get();
        Assert.assertEquals("ExceptionallyTest", r.currency);
    }

    /**
     * The DTO we send from the service. Not matching what we expect on the other side.
     */
    public static class DtoWeSend {
        // Serialized to:
        // "currency":{"currencyCode":"NOK"}
        public MyObject currency;

        public DtoWeSend() {
        }

        public DtoWeSend(String incomingMessage) {
            currency = new MyObject();
            currency.currencyCode = incomingMessage;
        }
    }

    public static class MyObject {
        // CHECKSTYLE IGNORE VisibilityModifier FOR NEXT 1 LINES
        public String currencyCode;

        public MyObject() {
        }
    }

    /**
     * The DTO we expect to receive. Not matching what the service is sending.
     */
    public static class DtoWeExpect {
        // This should throw an exception during deserialization as MyObject does not deserialize to String
        public String currency;

        public DtoWeExpect() {
        }

        public DtoWeExpect(String currency) {
            this.currency = currency;
        }
    }
}
