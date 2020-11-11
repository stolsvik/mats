package com.stolsvik.mats.lib_test;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;

import com.stolsvik.mats.junit.Rule_Mats;

/**
 * Super-class for the Basic MATS lib unit tests, providing some common infrastructure, including an instance of the
 * JUnit Rule {@link Rule_Mats}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public abstract class MatsBasicTest extends _MatsTestBase {
    @ClassRule
    public static Rule_Mats matsRule = Rule_Mats.createRule();

    @After
    public void cleanFactory() {
        matsRule.removeAllEndpoints();
    }
}
