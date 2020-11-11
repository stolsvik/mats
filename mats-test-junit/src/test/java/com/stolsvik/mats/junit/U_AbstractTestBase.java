package com.stolsvik.mats.junit;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import com.stolsvik.mats.util.MatsFuturizer;

/**
 * Example of a base test class which sets up everything needed to execute different MATS unit tests.
 *
 * @author Kevin Mc Tiernan, 2020-11-09, kmctiernan@gmail.com
 */
public abstract class U_AbstractTestBase {

    // :: Register the Rule_Mats, provides the MATS test harness.
    @ClassRule
    public static Rule_Mats __mats = Rule_Mats.createRule();

    // :: MatsFuturizer for request/reply scenarios.
    public MatsFuturizer _matsFuturizer;

    /**
     * Setup the {@link MatsFuturizer} for each new unit test.
     */
    @Before
    public void setupFuturizer() {
        _matsFuturizer = MatsFuturizer.createMatsFuturizer(__mats.getMatsFactory(), getClass().getSimpleName());
    }

    /**
     * Clean up the {@link MatsFuturizer} and {@link com.stolsvik.mats.MatsFactory} after each test.
     */
    @After
    public void cleanUpAfterTest() {
        _matsFuturizer.close();
        __mats.removeAllEndpoints();
    }
}
