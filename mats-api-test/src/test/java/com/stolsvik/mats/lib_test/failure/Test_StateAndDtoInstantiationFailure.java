package com.stolsvik.mats.lib_test.failure;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;

import com.stolsvik.mats.impl.jms.JmsMatsFactory.CannotInstantiateClassException;
import com.stolsvik.mats.test.junit.Rule_Mats;
import com.stolsvik.mats.test.MatsTestHelp;

/**
 * Tests that the early catching of non-instantiatable DTOs and State classes works.
 *
 * @author Endre Stølsvik 2019-10-27 22:02 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_StateAndDtoInstantiationFailure  {
    private static final Logger log = MatsTestHelp.getClassLogger();

    @ClassRule
    public static final Rule_Mats MATS = Rule_Mats.create();

    private static final String SERVICE = MatsTestHelp.service();

    public static class MissingNoArgsConstructor {
        public MissingNoArgsConstructor(String test) {
            /* no-op */
        }
    }

    public static class ExceptionInConstructor {
        ExceptionInConstructor() {
            throw new RuntimeException("Throw from Constructor!");
        }
    }

    @Before
    public void cleanMatsFactory() {
        MATS.cleanMatsFactory();
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void missingNoArgs_Endpoint_Reply() {
        MATS.getMatsFactory().single(SERVICE, MissingNoArgsConstructor.class, String.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void exceptionInConstructor_Endpoint_Reply() {
        MATS.getMatsFactory().single(SERVICE, ExceptionInConstructor.class, String.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void missingNoArgs_Stage_Incoming() {
        MATS.getMatsFactory().single(SERVICE, String.class, MissingNoArgsConstructor.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void exceptionInConstructor_Stage_Incoming() {
        MATS.getMatsFactory().single(SERVICE, String.class, ExceptionInConstructor.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void missingNoArgs_Endpoint_State() {
        MATS.getMatsFactory().staged(SERVICE, String.class, MissingNoArgsConstructor.class);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void exceptionInConstructor_Stage_State() {
        MATS.getMatsFactory().staged(SERVICE, String.class, ExceptionInConstructor.class);
    }
}
