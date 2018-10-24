package com.stolsvik.mats.lib_test.lifecycle;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Test_LateStart_And_WaitForStarted extends MatsBasicTest {
    private MatsEndpoint<Void, StateTO> _ep;
    private CountDownLatch _waitThreadStarted = new CountDownLatch(1);
    private CountDownLatch _waitedForStartupFinished = new CountDownLatch(1);

    @Before
    public void setupButDontStartTerminator() {
        _ep = matsRule.getMatsFactory().staged(TERMINATOR, Void.class, StateTO.class);
        _ep.stage(DataTO.class, (context, sto, dto) -> {
            log.debug("Stage:\n" + context.toString());
            matsTestLatch.resolve(sto, dto);
        });
    }

    @Test
    public void doTest() throws InterruptedException {
        DataTO dto = new DataTO(42, "TheAnswer");
        matsRule.getMatsFactory().createInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        .send(dto));


        // Make and start thread that will wait for Endpoint to start.
        Thread waiter = new Thread(() -> {
            _waitThreadStarted.countDown();
            _ep.waitForStarted();
            _waitedForStartupFinished.countDown();
        }, "UnitTestWaiter");
        waiter.start();

        // Now wait for the waiter-thread to actually fire up - should go instantly.
        boolean threadStarted = _waitThreadStarted.await(1000, TimeUnit.MILLISECONDS);
        // Assert that waiter-thread has not gotten past waiting
        Assert.assertTrue("Waiter thread should have started!", threadStarted);

        // Wait for the answer in 250 ms - which should not come.
        try {
            matsTestLatch.waitForResult(250);
            Assert.fail("This should not have happened, since the Endpoint isn't started yet.");
        }
        catch (AssertionError ae) {
            // This is good - we're expecting this "no-show".
        }

        // Assert that waiter-thread has not gotten past waiting
        Assert.assertEquals("Should not have gotten past waiting for ep to start, since we haven't started it!",
                1, _waitedForStartupFinished.getCount());

        // THIS IS IT! Start the endpoint!
        _ep.start();

        // Wait synchronously for terminator to finish, which now should happen pretty fast.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult(1000);
        Assert.assertEquals(dto, result.getData());

        // The waiter should either already have gotten through the waiting, or is in the process of doing so.
        boolean waitedFinishedOk = _waitedForStartupFinished.await(1000, TimeUnit.MILLISECONDS);

        // Assert that waiter-thread actually got through.
        Assert.assertTrue("The Waiter thread should have gotten through the waiting,"
                + " since we started the endpoint", waitedFinishedOk);

    }
}
