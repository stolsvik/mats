package com.stolsvik.mats.lib_test;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;

import com.stolsvik.mats.junit.Rule_MatsGeneric;
import com.stolsvik.mats.junit.Rule_MatsWithDb;

/**
 * Super-class for the Basic MATS lib unit tests, providing some common infrastructure, including an instance of the
 * JUnit Rule {@link Rule_MatsGeneric}.
 *
 * @author Endre Stølsvik - 2015-12-15 - http://endre.stolsvik.com
 */
public class MatsDbTest extends _MatsTestBase {
    @ClassRule
    // NOTICE: Rules must, for some damn annoying reason, be public.
    public static Rule_MatsWithDb matsRule = Rule_MatsWithDb.createRule();

    @After
    public void cleanUpFactory() {
        matsRule.removeAllEndpoints();
    }
}
