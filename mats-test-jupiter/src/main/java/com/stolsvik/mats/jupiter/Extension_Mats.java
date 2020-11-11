package com.stolsvik.mats.jupiter;

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
 * than {@link String}, then {@link Extension_MatsGeneric} offers this possibility.
 * <p>
 * {@link Extension_Mats} shall be annotated with {@link org.junit.jupiter.api.extension.RegisterExtension} and the
 * instance field shall be static for the Jupiter life cycle to pick up the extension at the correct time. {@link
 * Extension_Mats} can be viewed in the same manner as one would view a ClassRule in JUnit4.
 * <p>
 * Example:
 * <pre>
 *     public class YourTestClass {
 *         &#64;RegisterExtension
 *         public static Extension_Mats mats = Extension_Mats.createRule()
 *     }
 * </pre>
 * Note: Unlike the corresponding JUnit4 implementation, there is no requirement to add an explicit {@link
 * org.junit.jupiter.api.AfterEach} method executing a call to {@link Extension_Mats#removeAllEndpoints()} as the
 * increased versatility of Jupiter allows the extension to handle this correctly.
 *
 * @author Kevin Mc Tiernan, 2020-10-18, kmctiernan@gmail.com
 * @see Extension_MatsGeneric
 */
public class Extension_Mats extends Extension_MatsGeneric<String> {

    private Extension_Mats() {
        super(new MatsSerializer_DefaultJson());
    }

    private Extension_Mats(MatsSerializer<String> matsSerializer) {
        super(matsSerializer);
    }

    /**
     * Creates an {@link Extension_Mats} utilizing the {@link MatsSerializer_DefaultJson MATS default serializer}
     */
    public static Extension_Mats createRule() {
        return new Extension_Mats();
    }

    /**
     * Creates an {@link Extension_Mats} utilizing the user provided {@link MatsSerializer} which serializes to the type
     * of String.
     */
    public static Extension_Mats createRule(MatsSerializer<String> matsSerializer) {
        return new Extension_Mats(matsSerializer);
    }
}
