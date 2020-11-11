package com.stolsvik.mats.junit;

import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.test.abstractunit.AbstractMatsTest;

/**
 * Similar to {@link Rule_Mats}, provides a full MATS harness for unit testing by creating {@link JmsMatsFactory
 * MatsFactory} utilizing an in-vm Active MQ broker. The difference between the two is that this Rule is open to the
 * usage of more customized {@link MatsSerializer}s.
 * <p>
 * {@link Rule_MatsGeneric} shall be considered a {@link org.junit.ClassRule} and thus annotated as such, being a {@link
 * org.junit.ClassRule} also means that the instance field shall be static. Therefore to utilize {@link
 * Rule_MatsGeneric} one should add it to a test class in this fashion:
 * <pre>
 *     public class YourTestClass {
 *         &#64;ClassRule
 *         public static Rule_MatsGeneric&lt;Z&gt; mats = new Rule_MatsGeneric(new YourSerializer())
 *     }
 * </pre>
 * This will ensure that Rule_MatsGeneric sets up the test harness correctly. However the factory needs to be cleaned in
 * between tests to ensure that there are no endpoint collisions (One can only register a given endpointId once per
 * MatsFactory), thus one should also implement a method within the test class which is annotated with {@link
 * org.junit.After} and executes a call to {@link Rule_MatsGeneric#removeAllEndpoints()} as such:
 * <pre>
 *     &#64;After
 *     public void cleanFactory() {
 *         mats.removeAllEndpoints();
 *     }
 * </pre>
 *
 * @param <Z>
 *         The type definition for the {@link MatsSerializer} employed. This defines the type which STOs and DTOs are
 *         serialized into. When employing JSON for the "outer" serialization of MatsTrace, it does not make that much
 *         sense to use a binary (Z=byte[]) "inner" representation of the DTOs and STOs, because JSON is terrible at
 *         serializing byte arrays.
 * @author Kevin Mc Tiernan, 2020-10-22, kmctiernan@gmail.com
 * @see AbstractMatsTest
 */
public class Rule_MatsGeneric<Z> extends AbstractMatsTest<Z> implements JUnitLifeCycle {

    public Rule_MatsGeneric(MatsSerializer<Z> matsSerializer) {
        super(matsSerializer);
    }

    @Override
    public void beforeClass() {
        super.beforeAll();
    }

    @Override
    public void afterClass() {
        super.afterAll();
    }
}
