package com.stolsvik.mats.util;

import com.stolsvik.mats.MatsTrace;

/**
 * Defines the operations needed by MATS to serialize and deserialize both objects and {@link MatsTrace}s to and from
 * Strings (e.g. JSON or XML) - this can be employed separate from the communications mechanism (e.g. both the JMS and
 * RabbitMQ implementation can utilize the same serializer).
 * <p>
 * The default implementation is {@link MatsDefaultJsonSerializer}, employing Jackson JSON library to serialize to JSON.
 * <p>
 * It is worth pointing out that <i>all</i> the communicating parties needs to be using the same serialization
 * mechanism, as this constitute the "wire-representation" of the protocol that {@link MatsTrace} represents.
 * <p>
 * One can envision the protocol being serialized in a binary fashion, getting much smaller wire representations.
 *
 * @author Endre Stølsvik - 2015-07-22 - http://endre.stolsvik.com
 */
public interface MatsStringSerializer {
    /**
     * Used for serializing the {@link MatsTrace}. The {@link MatsTrace} class is inherently serializable directly, but
     * it is possible to implement different serialization schemes.
     *
     * @param matsTrace
     *            the {@link MatsTrace} instance to serialize.
     * @return a string representation.
     */
    String serializeMatsTrace(MatsTrace matsTrace);

    /**
     * @param string
     *            the String to reconstitute the {@link MatsTrace} from.
     * @return the reconstituted {@link MatsTrace}.
     */
    MatsTrace deserializeMatsTrace(String string);

    /**
     * Used for serializing STOs and DTOs into Strings.
     * <p>
     * Note that the {@link MatsDefaultJsonSerializer} handles String instances specially, in that they are returned
     * directly, not being fed through the JSON quoting process. The opposite (or same?) happens on
     * {@link #deserializeMatsTrace(String)}.
     * <p>
     * Also note that the ordinary serialization process for JSON also directly returns the toString()'ed values of any
     * {@link Number} or {@link Boolean}.
     *
     * @param object
     *            the object to serialize.
     * @return a String representation of the provided object.
     */
    String serializeObject(Object object);

    /**
     * Used for serializing deserialize Strings to STOs and DTOs.
     * <p>
     * Note that the {@link MatsDefaultJsonSerializer} handles String.class specially: If this is specified as type, you
     * will get the raw string no matter if it is JSON. This leads to an interesting possibility: You may for some
     * endpoint or endpoint stage specify that you take String.class, while the sender sends JSONed objects. You will
     * then get the JSON directly, enabling handling different types of incoming JSONs - sort of how like
     * java.lang.Object parameters can receive whatever.
     * <p>
     * If <code>null</code> is provided as the String parameter, then <code>null</code> shall be returned.
     *
     * @param string
     *            the String that should be deserialized into an object of Class T. If <code>null</code> is provided,
     *            then <code>null</code> shall be returned.
     * @param type
     *            the Class that the supplied String representation is thought to represent.
     *
     * @return the reconstituted Object.
     */
    <T> T deserializeObject(String string, Class<T> type);
}
