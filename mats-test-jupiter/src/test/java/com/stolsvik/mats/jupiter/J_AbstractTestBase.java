package com.stolsvik.mats.jupiter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.stolsvik.mats.util.MatsFuturizer;

/**
 * Example of a base test class which sets up everything needed to execute different MATS unit tests.
 *
 * @author Kevin Mc Tiernan, 2020-11-09, kmctiernan@gmail.com
 */
public class J_AbstractTestBase {

    // :: Register the Extension_Mats, provides the test harness.
    @RegisterExtension
    public static Extension_Mats __mats = Extension_Mats.createRule();

    // :: MatsFuturizer for request/reply scenarios.
    public MatsFuturizer _matsFuturizer;

    /**
     * Setup the {@link MatsFuturizer} for each new unit test.
     */
    @BeforeEach
    public void setupFuturizer() {
        _matsFuturizer = MatsFuturizer.createMatsFuturizer(__mats.getMatsFactory(), getClass().getSimpleName());
    }

    /**
     * Clean up the {@link MatsFuturizer}
     */
    @AfterEach
    public void cleanUpAfterTest() {
        _matsFuturizer.close();
    }
}
