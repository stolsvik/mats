package com.stolsvik.mats.lib_test;

import org.junit.Rule;

import com.stolsvik.mats.test.Rule_Mats;
import com.stolsvik.mats.test.Rule_MatsWithDb;

/**
 * Super-class for the Basic MATS lib unit tests, providing some common infrastructure, including an instance of the
 * JUnit Rule {@link Rule_Mats}.
 *
 * @author Endre Stølsvik - 2015-12-15 - http://endre.stolsvik.com
 */
public class MatsDbTest extends _MatsTestBase {
    @Rule
    // NOTICE: Rules must, for some damn annoying reason, be public.
    public Rule_MatsWithDb matsRule = new Rule_MatsWithDb();
}
