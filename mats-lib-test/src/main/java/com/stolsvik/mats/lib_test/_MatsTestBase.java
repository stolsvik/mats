package com.stolsvik.mats.lib_test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.test.MatsTestLatch;
import com.stolsvik.mats.util.RandomString;

/**
 * Common top-level class for the Mats JUnit tests - use the extensions {@link MatsBasicTest} and {@link MatsDbTest}
 * which also provides the Mats JUnit Rules.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class _MatsTestBase {
    protected Logger log = LoggerFactory.getLogger(getClass());

    protected String INITIATOR = getClass().getSimpleName() + ".INITIATOR";
    protected String SERVICE = getClass().getSimpleName() + ".SERVICE";
    protected String TERMINATOR = getClass().getSimpleName() + ".TERMINATOR";

    {
        log.info("### Instantiating class [" + this.getClass().getName() + "].");
    }

    protected MatsTestLatch matsTestLatch = new MatsTestLatch();

    /**
     * @return a random string of length 8 for testing - <b>BUT PLEASE NOTE!! ALWAYS use a semantically meaningful,
     *         globally unique Id as traceId in production code!</b>
     */
    protected String randomId() {
        return RandomString.randomString(8);
    }

    /**
     * Sleeps the specified number of milliseconds - can emulate processing time, primarily meant for the concurrency
     * tests.
     *
     * @param millis
     *            the number of millis to sleep
     * @throws AssertionError
     *             if an {@link InterruptedException} occurs.
     */
    protected void takeNap(int millis) throws AssertionError {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
