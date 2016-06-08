package com.stolsvik.mats.lib_test;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.test.MatsTestLatch;

/**
 * Common top-level class for the Mats JUnit tests - use the extensions {@link MatsBasicTest} and {@link MatsDbTest}
 * which also provides the Mats JUnit Rules.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class _MatsTestBase {
    protected Logger log = LoggerFactory.getLogger(getClass());

    protected String INITIATOR = getClass().getName() + ".INITIATOR";
    protected String SERVICE = getClass().getName() + ".SERVICE";
    protected String TERMINATOR = getClass().getName() + ".TERMINATOR";

    {
        log.info("### Instantiating class [" + this.getClass().getName() + "].");
    }

    protected MatsTestLatch matsTestLatch = new MatsTestLatch();

    /**
     * @return a random UUID string, to be used for traceIds when testing - <b>ALWAYS use a semantically meaningful,
     *         globally unique Id as traceId in production code!</b>
     */
    protected String randomId() {
        return UUID.randomUUID().toString();
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
