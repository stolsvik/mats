package com.stolsvik.mats.lib_test.basics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.lib_test.DataTO;
import com.stolsvik.mats.lib_test.MatsBasicTest;
import com.stolsvik.mats.lib_test.StateTO;

/**
 * Test sending 20 roughly 1 MB messages directly to a terminator. 1 MB messages should be considered pretty big. Using
 * the default compression level on the {@code MatsSerializer_DefaultJson} implementation {@link Deflater#BEST_SPEED},
 * the deflate operation takes just around 10ms for each of these simple messages (resulting size ~271kB), and total
 * production time (with serialization) is right about 16 ms. On the receiving size, the inflate operation takes about
 * 4.6 ms.
 *
 * @author Endre Stølsvik - 2018-10-25 - http://endre.stolsvik.com
 */
public class Test_LargeMessages extends MatsBasicTest {

    private static class LargeMessageDTO {
        private final int _index;
        private final List<DataTO> _dataTransferObjects;

        public LargeMessageDTO() {
            /* need no-args constructor due to Jackson */
            _index = 0;
            _dataTransferObjects = null;
        }

        public LargeMessageDTO(int index, List<DataTO> dataTransferObjects) {
            _index = index;
            _dataTransferObjects = dataTransferObjects;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LargeMessageDTO that = (LargeMessageDTO) o;
            return _index == that._index &&
                    Objects.equals(_dataTransferObjects, that._dataTransferObjects);
        }
    }

    private static int NUMBER_OF_MESSAGES = 20;
    private static int NUMBER_OF_DATATO_PER_MESSAGE = 11204; // Serializes to close around 1048576 bytes, which is 1MB.

    private CountDownLatch _latch = new CountDownLatch(NUMBER_OF_MESSAGES);

    private LargeMessageDTO[] _receivedMessages = new LargeMessageDTO[NUMBER_OF_MESSAGES];

    @Before
    public void setupTerminator() {
        matsRule.getMatsFactory().terminator(TERMINATOR, StateTO.class, LargeMessageDTO.class,
                config -> {
                    // Endpoint config - setting concurrency.
                    config.setConcurrency(7);
                },
                config -> {
                    // Stage config - nothing to do.
                }, (context, sto, dto) -> {
                    synchronized (_receivedMessages) {
                        _receivedMessages[dto._index] = dto;
                    }
                    _latch.countDown();
                });
    }

    @Test
    public void doTest() throws InterruptedException {
        List<LargeMessageDTO> messages = new ArrayList<>(NUMBER_OF_MESSAGES);
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            List<DataTO> messageContent = new ArrayList<>(NUMBER_OF_DATATO_PER_MESSAGE);
            for (int j = 0; j < NUMBER_OF_DATATO_PER_MESSAGE; j++) {
                messageContent.add(new DataTO(Math.random(), "Random:" + Math.random(), j));
            }
            messages.add(new LargeMessageDTO(i, messageContent));
        }
        MatsInitiator matsInitiator = matsRule.getMatsInitiator();
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            final int finalI = i;
            new Thread(() -> {
                matsInitiator.initiateUnchecked(
                        (msg) -> {
                            msg.traceId(randomId())
                                    .from(INITIATOR)
                                    .to(TERMINATOR)
                                    .send(messages.get(finalI));
                        });
            }).start();
        }

        // Wait synchronously for terminator to finish.
        boolean gotIt = _latch.await(10, TimeUnit.SECONDS);
        if (!gotIt) {
            throw new AssertionError("Didn't get all " + NUMBER_OF_MESSAGES + " messages in 10 seconds!");
        }

        Assert.assertArrayEquals(messages.toArray(), _receivedMessages);
    }
}
