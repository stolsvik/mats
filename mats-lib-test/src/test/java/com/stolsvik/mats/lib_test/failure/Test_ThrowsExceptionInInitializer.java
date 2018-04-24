package com.stolsvik.mats.lib_test.failure;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsDbTest;
import com.stolsvik.mats.lib_test.StateTO;

/**
 * Tests throwing inside the initiator, which should "propagate all the way out", while the about-to-be-sent message
 * will be rolled back and not be sent anyway. Note that in the logs, the situation will be logged on error by the MATS
 * implementation, but there will be no post on the Dead Letter Queue, since we're not in a message reception situation.
 * <p>
 * ASCII-artsy, it looks like this:
 *
 * <pre>
 * [Initiator]    - init request, but throws RuntimeException, which should propagate all the way out.
 * [Terminator] - should not get the message (but we have a test asserting it will actually get it if do NOT throw!)
 * </pre>
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Test_ThrowsExceptionInInitializer extends MatsDbTest {
    private static int WAIT_MILLIS = 500;

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, DataTO.class, StateTO.class,
                (context, dto, sto) -> matsTestLatch.resolve(dto, sto));
    }

    /**
     * Tests that an exception is thrown in the initiation block will propagate out of the initiator.
     */
    @Test(expected = TestRuntimeException.class)
    public void exceptionInInitiationShouldPropagateOut() {
        matsRule.getMatsFactory().getInitiator().initiate(
                (msg) -> {
                    throw new TestRuntimeException("Should propagate all the way out.");
                });
    }

    /**
     * Tests that the infrastructure for checking that a message is NOT received is actually working, by sending a
     * message using the same code which we assert that we DO receive!
     */
    @Test
    public void checkTestInfrastructre() {
        sendMessageToTerminator(false);
        Assert.assertNotNull(matsTestLatch.waitForResult(WAIT_MILLIS));
    }

    /**
     * Tests that if an exception is thrown in the initiation block, any sent messages will be rolled back.
     */
    @Test
    public void exceptionInInitiationShouldNotSendMessage() {
        sendMessageToTerminator(true);

        // Wait synchronously for terminator to finish.
        try {
            matsTestLatch.waitForResult(WAIT_MILLIS);
        }
        // NOTE! The MatsTestLatch throws AssertionError when the wait time overruns.
        catch (AssertionError e) {
            // Good that we came here! - we should NOT receive the message on the Terminator, due to init Exception.
            log.info("We as expected dit NOT get the message that was sent in the initiator!");
            return;
        }
        Assert.fail("Should NOT have gotten message!");
    }

    private void sendMessageToTerminator(boolean throwInInitiation) {
        DataTO dto = new DataTO(42, "TheAnswer");
        try {
            matsRule.getMatsFactory().getInitiator().initiate(
                    (msg) -> {
                        msg.traceId(randomId())
                                .from(INITIATOR)
                                .to(TERMINATOR)
                                .send(dto);
                        if (throwInInitiation) {
                            throw new TestRuntimeException("Should rollback the initiation, and not send message.");
                        }
                    });
        }
        catch (TestRuntimeException e) {
            log.info("Got expected " + e.getClass().getSimpleName() + " from the MATS Initiation.");
        }
    }

    private static class TestRuntimeException extends RuntimeException {
        public TestRuntimeException(String message) {
            super(message);
        }
    }
}
