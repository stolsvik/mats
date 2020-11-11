package com.stolsvik.mats.junit;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.stolsvik.mats.test.abstractunit.AbstractMatsTest;
import com.stolsvik.mats.test.abstractunit.AbstractMatsTestEndpoint;

/**
 * Common interface for classes wishing to be annotated with either {@link org.junit.ClassRule} or
 * {@link org.junit.Rule} (Also supports rules wishing to utilize both in combination.).
 * <p>
 * Normally one would extended {@link org.junit.rules.ExternalResource} to hook a custom rule into the JUnit life cycle,
 * however, since both {@link Rule_MatsGeneric} and {@link Rule_MatsEndpoint} both utilize common code by extending
 * {@link AbstractMatsTest} and {@link AbstractMatsTestEndpoint} respectively, this interface was created to allow for
 * the extension of these classes while also being able to implement this interface providing a hook into the JUnit
 * life cycle.
 * <p>
 * This interface extends {@link TestRule} which is the hook to JUnit and provides the following no-op methods:
 * <ul>
 *     <li>{@link #beforeClass()} - Utilized by {@link org.junit.ClassRule}</li>
 *     <li>{@link #afterClass()} - Utilized by {@link org.junit.ClassRule}</li>
 *     <li>{@link #before()} - Utilized by {@link org.junit.Rule}</li>
 *     <li>{@link #after()} - Utilized by {@link org.junit.Rule}</li>
 * </ul>
 * Note: Copied shamelessly from:
 * <a href="https://stackoverflow.com/a/48759584">How to combine @Rule and @ClassRule in JUnit 4.12</a>
 *
 * @author Kevin Mc Tiernan, 2020-10-22, kmctiernan@gmail.com
 */
public interface JUnitLifeCycle extends TestRule {

    @Override
    default Statement apply(Statement base, Description description) {
        // :: Rule handling.
        if (description.isTest()) {
            return new Statement() {
                public void evaluate() throws Throwable {
                    before();
                    try {
                        base.evaluate();
                    }
                    finally {
                        after();
                    }
                }
            };
        }
        // :: ClassRule handling.
        if (description.isSuite()) {
            return new Statement() {
                public void evaluate() throws Throwable {
                    beforeClass();
                    try {
                        base.evaluate();
                    }
                    finally {
                        afterClass();
                    }
                }
            };
        }
        return base;
    }

    default void beforeClass() throws Throwable {
        // NOOP
    }

    default void before() throws Throwable {
        // NOOP
    }

    default void after() {
        // NOOP
    }

    default void afterClass() {
        // NOOP
    }


}
