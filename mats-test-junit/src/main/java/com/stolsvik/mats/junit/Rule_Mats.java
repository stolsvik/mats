package com.stolsvik.mats.junit;

import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;

/**
 * Provides a full MATS harness for unit testing by creating {@link JmsMatsFactory MatsFactory} utilizing an in-vm
 * Active MQ broker.
 * <p>
 * By default the {@link #createRule() rule} will create a {@link MatsSerializer_DefaultJson} which will be the
 * serializer utilized by the created {@link JmsMatsFactory MatsFactory}. Should one want to use a different serializer
 * which serializes to the type of {@link String} then this can be specified using the method {@link
 * #createRule(MatsSerializer)}. However should one want to specify a serializer which serializes into anything other
 * than {@link String}, then {@link Rule_MatsGeneric} offers this possibility.
 * <p>
 * {@link Rule_Mats} shall be considered a {@link org.junit.ClassRule} and thus annotated as such, being a {@link
 * org.junit.ClassRule} also means that the instance field shall be static. Therefore to utilize {@link Rule_Mats} one
 * should add it to a test class in this fashion:
 * <pre>
 *     public class YourTestClass {
 *         &#64;ClassRule
 *         public static Rule_Mats mats = Rule_Mats.createRule()
 *     }
 * </pre>
 * This will ensure that Rule_Mats sets up the test harness correctly. However the factory needs to be cleaned in
 * between tests to ensure that there are no endpoint collisions (One can only register a given endpointId once per
 * MatsFactory), thus one should also implement a method within the test class which is annotated with {@link
 * org.junit.After} and executes a call to {@link Rule_Mats#removeAllEndpoints()} as such:
 * <pre>
 *     &#64;After
 *     public void cleanFactory() {
 *         mats.removeAllEndpoints();
 *     }
 * </pre>
 *
 * @author Kevin Mc Tiernan, 2020-10-18, kmctiernan@gmail.com
 * @see Rule_MatsGeneric
 */
public class Rule_Mats extends Rule_MatsGeneric<String> implements JUnitLifeCycle {

    private Rule_Mats() {
        super(new MatsSerializer_DefaultJson());
    }

    private Rule_Mats(MatsSerializer<String> matsSerializer) {
        super(matsSerializer);
    }

    /**
     * Creates a {@link Rule_Mats} utilizing the {@link MatsSerializer_DefaultJson MATS default serializer}
     */
    public static Rule_Mats createRule() {
        return new Rule_Mats();
    }

    /**
     * Creates a {@link Rule_Mats} utilizing the user provided {@link MatsSerializer} which serializes to the type of
     * String.
     */
    public static Rule_Mats createRule(MatsSerializer<String> matsSerializer) {
        return new Rule_Mats(matsSerializer);
    }
}
