package com.stolsvik.mats.serial;

/**
 * Defines the operations needed by MATS to serialize and deserialize {@link MatsTrace}s to and from byte arrays (e.g.
 * UTF-8 encoded JSON or XML, or some binary serialization protocol), and STOs and DTOs to and from type Z, where Z can
 * e.g. be byte arrays or Strings. This is separated out from the MATS communication implementation (i.e. JMS or
 * RabbitMQ), as it is a separate aspect, i.e. both the JMS and RabbitMQ implementation can utilize the same serializer.
 * <p>
 * The default implementation in mats-serial-json (<code>MatsSerializer_DefaultJson</code>) employs the Jackson JSON
 * library to serialize to JSON.
 * <p>
 * It is worth pointing out that <i>all</i> the communicating parties needs to be using the same serialization
 * mechanism, as this constitute the "wire-representation" of the protocol that {@link MatsTrace} represents.
 *
 * @param <Z>
 *            The type which STOs and DTOs are serialized into.
 *
 * @author Endre Stølsvik - 2015-07-22 - http://endre.stolsvik.com
 */
public interface MatsSerializer<Z> {
    /**
     * Used when initiating a new MATS processing. Since the {@link MatsTrace} implementation is serialization mechanism
     * implementation dependent, we need a way to instantiate new instances of the implementation of MatsTrace.
     *
     * @param traceId
     *            the Trace Id of this new {@link MatsTrace}.
     * @param keepTrace
     *            whether the MatsTrace should "keep trace", i.e. not used "compressed mode", i.e. whether all Calls and
     *            States should be kept through the entire flow from initiation to terminator - default shall be
     *            <code>false</code>. The only reason for why this exists is for debugging: The implementation cannot
     *            depend on this feature.
     * @param nonPersistent
     *            whether the message should be JMS-style "non-persistent" - default shall be <code>false</code>, i.e.
     *            the default is that a message is persistent.
     * @param interactive
     *            whether the message should be prioritized in that a human is actively waiting for the reply, default
     *            shall be <code>false</code>.
     *
     * @return a new instance of the underlying {@link MatsTrace} implementation.
     */
    MatsTrace<Z> createNewMatsTrace(String traceId, boolean keepTrace, boolean nonPersistent, boolean interactive);

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
     * If <code>null</code> is provided as the Object parameter, then <code>null</code> shall be returned.
     *
     * @param object
     *            the object to serialize. If <code>null</code> is provided, then <code>null</code> shall be returned.
     * @return a String representation of the provided object, or <code>null</code> if null was provided as 'object'.
     */
    Z serializeObject(Object object);

    /**
     * Used for deserializing type Z (typically {@link String}) to STOs and DTOs.
     * <p>
     * If <code>null</code> is provided as the 'Z serialized' parameter, then <code>null</code> shall be returned.
     *
     * @param serialized
     *            the value of type T that should be deserialized into an object of Class T. If <code>null</code> is
     *            provided, then <code>null</code> shall be returned.
     * @param type
     *            the Class that the supplied value of type Z is thought to represent (i.e. the STO or DTO class).
     *
     * @return the reconstituted Object (STO or DTO), or <code>null</code> if null was provided as 'serialized'.
     */
    <T> T deserializeObject(Z serialized, Class<T> type);

    /**
     * Will return a new instance of the requested type. This is used to instantiate "empty objects" for state
     * (STOs). The reason for having this in the MatsSerializer is that it is somewhat dependent on the object
     * serializer in use: GSON allows to instantiate private, missing-no-args-constructor classes, while Jackson does
     * not.
     *
     * @param type Which class you want an object of.
     * @param <T> the type of that class.
     * @return an "empty" new instance of the class.
     */
    <T> T newInstance(Class<T> type);

    /**
     * The methods in this interface shall throw this RuntimeException if they encounter problems.
     */
    class SerializationException extends RuntimeException {
        public SerializationException(String message) {
            super(message);
        }

        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
