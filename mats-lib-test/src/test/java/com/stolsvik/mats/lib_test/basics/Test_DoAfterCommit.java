package com.stolsvik.mats.lib_test.basics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Simple test to check that {@link ProcessContext#doAfterCommit(Runnable)} is run.
 */
public class Test_DoAfterCommit extends MatsBasicTest {

    private CountDownLatch _countDownLatch = new CountDownLatch(1);

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                    context.doAfterCommit(() -> _countDownLatch.countDown());
                });
    }

    @Test
    public void doTest() throws InterruptedException {
        DataTO dto = new DataTO(10, "ThisIsIt");
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(TERMINATOR)
                        .send(dto));

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(dto, result.getData());

        // Wait for the doAfterCommit lambda runs.
        boolean await = _countDownLatch.await(1, TimeUnit.SECONDS);
        Assert.assertTrue("The doAfterCommit() lambda didn't occur!", await);
    }
}
