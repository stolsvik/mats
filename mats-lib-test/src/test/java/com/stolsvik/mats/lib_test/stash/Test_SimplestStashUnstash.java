package com.stolsvik.mats.lib_test.stash;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsInitiator.MatsBackendException;
import com.stolsvik.mats.MatsInitiator.MatsMessageSendException;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;
import com.stolsvik.mats.test.MatsTestLatch.Result;

/**
 * Simplest example of stash/unstash: "Single-stage" that employs stash.
 *
 * @author Endre Stølsvik - 2018-10-23 - http://endre.stolsvik.com
 */
public class Test_SimplestStashUnstash extends MatsBasicTest {

    private byte[] _stash;
    private CountDownLatch _stashLatch = new CountDownLatch(1);

    @Before
    public void setupService() {
        MatsEndpoint<DataTO, StateTO> staged = matsRule.getMatsFactory().staged(SERVICE, DataTO.class, StateTO.class);
        // Cannot employ a single-stage, since that requires a reply (by returning something, even null).
        // Thus, employing multistage, with only one stage, where we do not invoke context.reply(..)
        staged.stage(DataTO.class, ((context, state, incomingDto) -> {
            _stash = context.stash();
            _stashLatch.countDown();
        }));
        staged.finishSetup();
    }

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, DataTO.class,
                (context, sto, dto) -> {
                    log.debug("TERMINATOR MatsTrace:\n" + context.toString());
                    matsTestLatch.resolve(sto, dto);
                });

    }

    @Test
    public void doTest() throws InterruptedException, MatsMessageSendException, MatsBackendException {
        DataTO dto = new DataTO(42, "TheAnswer");
        StateTO sto = new StateTO(420, 420.024);
        matsRule.getMatsInitiator().initiateUnchecked(
                (msg) -> msg.traceId(randomId())
                        .from(INITIATOR)
                        .to(SERVICE)
                        .replyTo(TERMINATOR, sto)
                        .request(dto));

        // Wait synchronously for stash to appear
        _stashLatch.await(1, TimeUnit.SECONDS);

        // Unstash!
        matsRule.getMatsInitiator().unstash(_stash, DataTO.class, StateTO.class, DataTO.class,
                (context, state, incomingDto) -> {
                    context.reply(new DataTO(dto.number * 2, dto.string + ":FromService"));
                });

        // Wait synchronously for terminator to finish.
        Result<StateTO, DataTO> result = matsTestLatch.waitForResult();
        Assert.assertEquals(sto, result.getState());
        Assert.assertEquals(new DataTO(dto.number * 2, dto.string + ":FromService"), result.getData());
    }
}