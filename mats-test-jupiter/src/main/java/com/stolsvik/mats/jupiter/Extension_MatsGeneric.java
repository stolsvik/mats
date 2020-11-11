package com.stolsvik.mats.jupiter;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.test.abstractunit.AbstractMatsTest;

/**
 * Similar to {@link Extension_Mats}, provides a full MATS harness for unit testing by creating {@link JmsMatsFactory
 * MatsFactory} utilizing an in-vm Active MQ broker. The difference between the two is that this Extension is open to
 * the usage of more customized {@link MatsSerializer}s.
 * <p>
 * {@link Extension_MatsGeneric} shall be annotated with {@link org.junit.jupiter.api.extension.RegisterExtension} and
 * the instance field shall be static for the Jupiter life cycle to pick up the extension at the correct time. {@link
 * Extension_MatsGeneric} can be viewed in the same manner as one would view a ClassRule in JUnit4.
 * <pre>
 *     public class YourTestClass {
 *         &#64;RegisterExtension
 *         public static Extension_MatsGeneric&lt;Z&gt; mats = new Extension_MatsGeneric(new YourSerializer())
 *     }
 * </pre>
 * This will ensure that Extension_MatsGeneric sets up the test harness correctly.
 *
 * @param <Z>
 *         The type definition for the {@link MatsSerializer} employed. This defines the type which STOs and DTOs are
 *         serialized into. When employing JSON for the "outer" serialization of MatsTrace, it does not make that much
 *         sense to use a binary (Z=byte[]) "inner" representation of the DTOs and STOs, because JSON is terrible at
 *         serializing byte arrays.
 * @author Kevin Mc Tiernan, 2020-10-22, kmctiernan@gmail.com
 * @see AbstractMatsTest
 */
public class Extension_MatsGeneric<Z> extends AbstractMatsTest<Z>
        implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    public Extension_MatsGeneric(MatsSerializer<Z> matsSerializer) {
        super(matsSerializer);
    }

    /**
     * Executed by Jupiter before any test method is executed. (Once at the start of the class.)
     */
    @Override
    public void beforeAll(ExtensionContext context) {
        super.beforeAll();
    }

    /**
     * Executed by Jupiter after each test method.
     */
    @Override
    public void afterEach(ExtensionContext context) {
        removeAllEndpoints();
    }

    /**
     * Executed by Jupiter after all test methods have been executed.
     */
    @Override
    public void afterAll(ExtensionContext context) {
        super.afterAll();
    }
}
