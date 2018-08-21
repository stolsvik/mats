package com.stolsvik.mats.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.stolsvik.mats.exceptions.MatsRefuseMessageException;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.util.MatsSynchronousAdapter.Reply;

/**
 * Tests timeouts functionality of the {@link MatsSynchronousAdapter}.
 */
public class Test_MatsSynchronousAdapterTimeouts extends MatsBasicTest {

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, msg) -> {
                    log.info("Inside SERVICE which will wait 250 ms, context:\n" + context);
                    try {
                        Thread.sleep(250);
                    }
                    catch (InterruptedException e) {
                        throw new MatsRefuseMessageException("Shouldn't get Interrupted here.");
                    }
                    return new DataTO(msg.number * 2, msg.string + ":FromService");
                });
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shallTimeoutInsideSynchronousAdapter() throws ExecutionException, InterruptedException,
            TimeoutException {
        // NOTE: Using try-with-resources in this test - not to be used in normal circumstances.
        try (MatsSynchronousAdapter<DataTO> synchronousAdapter = MatsSynchronousAdapter.create(matsRule
                .getMatsFactory(), matsRule.getMatsInitiator(), this.getClass().getSimpleName(), DataTO.class)) {

            DataTO dto = new DataTO(42, "TheAnswer");

            CompletableFuture<Reply<DataTO>> future = synchronousAdapter.futureRequest(75, randomId(),
                    INITIATOR, SERVICE, dto, (msg) -> {});

            // This timeout is raised by the SynchronousAdapter, since the future was created with a too short
            // timeout for the service to complete. The timeout-thread inside the SynchronousAdapter will thus
            // complete this future exceptionally, i.e. ExecutionException.
            thrown.expect(ExecutionException.class);

            // This shall not go through, but throw
            Reply<DataTO> result = future.get(1, TimeUnit.SECONDS);

            Assert.fail("The future.get(1 sec) call should not have gone through, since the future was created with"
                    + " only 75 milliseconds to wait for the completion of the service. The result was: " + result);
        }
    }

    @Test
    public void shallTimeoutInTheFutureGetInvocation() throws ExecutionException, InterruptedException,
            TimeoutException {
        // NOTE: Using try-with-resources in this test - not to be used in normal circumstances.
        try (MatsSynchronousAdapter<DataTO> synchronousAdapter = MatsSynchronousAdapter.create(matsRule
                .getMatsFactory(), matsRule.getMatsInitiator(), this.getClass().getSimpleName(), DataTO.class)) {

            DataTO dto = new DataTO(42, "TheAnswer");

            CompletableFuture<Reply<DataTO>> future = synchronousAdapter.futureRequest(2000, randomId(),
                    INITIATOR, SERVICE, dto, (msg) -> {});

            // This timeout is raised "synchronously" by the future.get() call itself, since it specified a too short
            // wait for the service to answer (the SynchronousAdapter waits 2 seconds, so that is sufficient). Since
            // it is the get()-call itself that throws, a TimeoutException will be raised.
            thrown.expect(TimeoutException.class);

            // This shall not go through, but throw
            Reply<DataTO> result = future.get(75, TimeUnit.MILLISECONDS);

            Assert.fail("The future.get(75 ms) call should not have gone through, since the timeout specified is"
                    + " too short for the service to answer. The result was: " + result);
        }
    }

}
