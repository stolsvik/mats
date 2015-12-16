package com.stolsvik.mats.lib_test;

import org.junit.Rule;

import com.stolsvik.mats.test.Rule_Mats;

/**
 * Super-class for the Basic MATS lib unit tests, providing some common infrastructure, including an instance of the
 * JUnit Rule {@link Rule_Mats}.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public abstract class MatsBasicTest extends _MatsTestBase {
    @Rule
    public Rule_Mats matsRule = new Rule_Mats();
}
