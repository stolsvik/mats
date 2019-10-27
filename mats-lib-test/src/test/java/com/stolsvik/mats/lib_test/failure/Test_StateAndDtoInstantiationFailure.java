package com.stolsvik.mats.lib_test.failure;

import org.junit.Test;

import com.stolsvik.mats.impl.jms.JmsMatsFactory.CannotInstantiateClassException;
import com.stolsvik.mats.lib_test.MatsBasicTest;

/**
 * Tests that the early catching of non-instantiatable DTOs and State classes works.
 *
 * @author Endre Stølsvik 2019-10-27 22:02 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Test_StateAndDtoInstantiationFailure extends MatsBasicTest {

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

    @Test(expected = CannotInstantiateClassException.class)
    public void missingNoArgs_Endpoint_Reply() {
        matsRule.getMatsFactory().single(SERVICE, MissingNoArgsConstructor.class, String.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void exceptionInConstructor_Endpoint_Reply() {
        matsRule.getMatsFactory().single(SERVICE, ExceptionInConstructor.class, String.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void missingNoArgs_Stage_Incoming() {
        matsRule.getMatsFactory().single(SERVICE, String.class, MissingNoArgsConstructor.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void exceptionInConstructor_Stage_Incoming() {
        matsRule.getMatsFactory().single(SERVICE, String.class, ExceptionInConstructor.class, (processContext,
                incomingDto) -> null);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void missingNoArgs_Endpoint_State() {
        matsRule.getMatsFactory().staged(SERVICE, String.class, MissingNoArgsConstructor.class);
    }

    @Test(expected = CannotInstantiateClassException.class)
    public void exceptionInConstructor_Stage_State() {
        matsRule.getMatsFactory().staged(SERVICE, String.class, ExceptionInConstructor.class);
    }

}
