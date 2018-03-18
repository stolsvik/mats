package com.stolsvik.mats.util.com.stolsvik.mats.impl.serial;

import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.util.com.stolsvik.mats.impl.serial.json.MatsSerializer_DefaultJson;

/**
 * Defines the operations needed by MATS to serialize and deserialize {@link MatsTrace}s to and from byte arrays
 * (e.g. UTF-8 encoded JSON or XML, or some binary serialization protocol), and STOs and DTOs to and from type Z,
 * where Z can e.g. be byte arrays or Strings. This is separated out from the MATS communication implementation (i.e.
 * JMS or RabbitMQ), as it is a separate aspect, i.e. both the JMS and RabbitMQ implementation can utilize the same
 * serializer.
 * <p>
 * The default implementation is {@link MatsSerializer_DefaultJson}, employing Jackson JSON library to serialize to JSON.
 * <p>
 * It is worth pointing out that <i>all</i> the communicating parties needs to be using the same serialization
 * mechanism, as this constitute the "wire-representation" of the protocol that {@link MatsTrace} represents.
 * <p>
 * One can envision the protocol being serialized in a binary fashion, getting much smaller wire representations.
 *
 * @param <Z> The type which STOs and DTOs are serialized into.
 *
 * @author Endre Stølsvik - 2015-07-22 - http://endre.stolsvik.com
 */
public interface MatsSerializer<Z> {

    /**
     * Used when initiating a new MATS processing. Since the {@link MatsTrace} implementation is serialization mechanism
     * implementation dependent, we need a way to instantiate new instances of the implementation of MatsTrace.
     *
     * @param traceId
     *            the {@link MatsInitiate#traceId(String) Trace Id} of this MatsTrace.
     * @return a new instance of the underlying {@link MatsTrace} implementation.
     */
    MatsTrace<Z> createNewMatsTrace(String traceId);

    /**
     * Used for serializing the {@link MatsTrace} to a byte array.
     *
     * @param matsTrace
     *            the {@link MatsTrace} instance to serialize.
     * @return a byte array representation of the provided {@link MatsTrace}.
     */
    byte[] serializeMatsTrace(MatsTrace<Z> matsTrace);

    /**
     * Used for deserializing a byte array into a {@link MatsTrace}
     *
     * @param serialized
     *            the byte array from which to reconstitute the {@link MatsTrace}.
     * @return the reconstituted {@link MatsTrace}.
     */
    MatsTrace<Z> deserializeMatsTrace(byte[] serialized);

    /**
     * Used for serializing STOs and DTOs into type Z, typically {@link String}.
     * <p>
     * Note that the {@link MatsSerializer_DefaultJson} handles String instances specially, in that they are returned
     * directly, not being fed through the JSON quoting process. The same happens on
     * {@link #deserializeObject(Object, Class)}.
     * <p>
     * Also note that the ordinary serialization process for JSON also directly returns the toString()'ed values of any
     * {@link Number} or {@link Boolean}.
     * <p>
     * If <code>null</code> is provided as the Object parameter, then <code>null</code> shall be returned.
     *
     * @param object
     *            the object to serialize. If <code>null</code> is provided, then <code>null</code> shall be returned.
     * @return a String representation of the provided object.
     */
    Z serializeObject(Object object);

    /**
     * Used for deserializing type Z (typically {@link String}) to STOs and DTOs.
     * <p>
     * Note that the {@link MatsSerializer_DefaultJson} handles String.class specially: If this is specified as type, you
     * will get the raw string no matter if it is JSON. This leads to an interesting possibility: You may for some
     * endpoint or endpoint stage specify that you take String.class, while the sender sends JSONed objects. You will
     * then get the JSON directly, enabling handling different types of incoming JSONs - sort of how like
     * java.lang.Object parameters can receive whatever.
     * <p>
     * If <code>null</code> is provided as the 'Z serialized' parameter, then <code>null</code> shall be returned.
     *
     * @param serialized
     *            the value of type T that should be deserialized into an object of Class T. If <code>null</code> is
     *            provided, then <code>null</code> shall be returned.
     * @param type
     *            the Class that the supplied value of type Z is thought to represent (i.e. the STO or DTO class).
     *
     * @return the reconstituted Object (STO or DTO).
     */
    <T> T deserializeObject(Z serialized, Class<T> type);
}
