package com.stolsvik.mats.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.util.MatsSynchronousAdapter.Reply;

/**
 * Tests standard functionality of the {@link MatsSynchronousAdapter}. Also tests sending null message, and receiving
 * null reply.
 */
public class Test_BasicMatsSynchronousAdapter extends MatsBasicTest {

    private static final Logger log = LoggerFactory.getLogger(Test_BasicMatsSynchronousAdapter.class);

    @Before
    public void setupService() {
        matsRule.getMatsFactory().single(SERVICE, DataTO.class, DataTO.class,
                (context, msg) -> {
                    log.info("Inside SERVICE, context:\n" + context);
                    if (msg == null) {
                        return null;
                    }
                    return new DataTO(msg.number * 2, msg.string + ":FromService");
                });
    }

    @Test
    public void normalMessage() throws ExecutionException, InterruptedException, TimeoutException {
        // NOTE: Using try-with-resources in this test - not to be used in normal circumstances.
        try (MatsSynchronousAdapter<DataTO> synchronousAdapter = MatsSynchronousAdapter.create(matsRule
                .getMatsFactory(), matsRule.getMatsInitiator(), this.getClass().getSimpleName(), DataTO.class)) {

            DataTO dto = new DataTO(42, "TheAnswer");

            CompletableFuture<Reply<DataTO>> future = synchronousAdapter.futureRequest(500, randomId(),
                    INITIATOR, SERVICE, dto, msg -> {
                        // Just check that the MatsInitiate is available.
                        msg.interactive();
                        msg.nonPersistent();
                    });

            Reply<DataTO> result = future.get(1, TimeUnit.SECONDS);

            Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getReply());

            log.info("Got the reply from the Future - the latency was " + result.getLatencyMillis() + " milliseconds");
        }
    }

    @Test
    public void nullMessageAndNullReply() throws ExecutionException, InterruptedException, TimeoutException {
        // NOTE: Using try-with-resources in this test - not to be used in normal circumstances.
        try (MatsSynchronousAdapter<DataTO> synchronousAdapter = MatsSynchronousAdapter.create(matsRule
                .getMatsFactory(), matsRule.getMatsInitiator(), this.getClass().getSimpleName(), DataTO.class)) {

            CompletableFuture<Reply<DataTO>> future = synchronousAdapter.futureRequest(500, randomId(),
                    INITIATOR, SERVICE, null, (msg) -> {});

            Reply<DataTO> result = future.get(1, TimeUnit.SECONDS);

            Assert.assertNull(result.getReply());

            log.info("Got the reply from the Future - the latency was " + result.getLatencyMillis() + " milliseconds");
        }
    }

}
