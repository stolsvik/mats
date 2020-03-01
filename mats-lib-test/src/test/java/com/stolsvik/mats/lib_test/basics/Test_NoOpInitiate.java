package com.stolsvik.mats.lib_test.basics;

import org.junit.Test;

import com.stolsvik.mats.lib_test.MatsBasicTest;

/**
 * Tests that it is OK to NOT do anything in initiate.
 *
 * @author Endre Stølsvik 2020-03-01 23:31 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_NoOpInitiate extends MatsBasicTest {
    @Test
    public void doTest() {
        matsRule.getMatsInitiator().initiateUnchecked(init -> {
            /* no-op: Not sending/requesting/doing anything .. */
        });
    }
}
