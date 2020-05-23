package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketEnvelopeWithMetaDto;

/**
 * @author Endre Stølsvik 2020-01-15 08:38 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsSocketStatics {

    String MDC_SESSION_ID = "matssocket.sessionId";
    String MDC_PRINCIPAL_NAME = "matssocket.principal";
    String MDC_USER_ID = "matssocket.userId";

    String MDC_CLIENT_LIB_AND_VERSIONS = "matssocket.clientLib";
    String MDC_CLIENT_APP_NAME_AND_VERSION = "matssocket.clientApp";

    String MDC_MESSAGE_TYPE = "matssocket.msgType";
    String MDC_TRACE_ID = "traceId";

    // Limits:
    int MAX_LENGTH_OF_TOPIC_NAME = 256;

    int MAX_NUMBER_OF_TOPICS_PER_SESSION = 1500;
    int MAX_NUMBER_OF_SESSIONS_PER_USER_ID = 75;
    int MAX_NUMBER_OF_RECORDED_ENVELOPES_PER_SESSION = 200;
    int MAX_NUMBER_OF_HELD_ENVELOPES_PER_SESSION = 100;
    int MAX_SIZE_OF_HELD_ENVELOPE_MSGS = 20 * 1024 * 1024;

    int MAX_NUMBER_OF_REDELIVERY_ATTEMPTS = 10;

    int MAX_NUMBER_OF_MESSAGES_PER_FORWARD_LOOP = 20;

    int MIN_FORWARDER_CORE_POOL_SIZE = 5;
    int MIN_FORWARDER_MAX_POOL_SIZE = 100;

    // Constants:
    long MILLIS_BETWEEN_LIVELINESS_UPDATE_RUN = 54 * 1000; // <One minute (54 sec + Random(10%))
    long MILLIS_BETWEEN_SESSION_TIMEOUT_RUN = 5 * 60 * 1000; // Five minutes

    long MILLIS_BETWEEN_SCAVENGE_SESSION_REMNANTS_RUN = 60 * 60 * 1000; // One hour
    // Sessions times out if last liveliness was three days ago
    Supplier<Long> MILLIS_SESSION_TIMEOUT_SUPPLIER = () -> System.currentTimeMillis() - 3 * 24 * 60 * 60 * 1000L;

    long MILLIS_BETWEEN_ACK_FLUSHES = 100;

    String THREAD_PREFIX = "MatsSocket: ";

    default double ms(long nanos) {
        return Math.round(nanos / 10_000d) / 1_00d;
    }

    default double msSince(long nanosStart) {
        return ms(System.nanoTime() - nanosStart);
    }

    class DebugStackTrace extends Exception {
        public DebugStackTrace(String what) {
            super("Debug Stacktrace to record where " + what + " happened.");
        }
    }

    default ObjectMapper jacksonMapper() {
        /*
         * NOTE: This following is stolen directly from MatsSerializer_DefaultJson - uses same serialization setup
         */
        ObjectMapper mapper = new ObjectMapper();

        // Read and write any access modifier fields (e.g. private)
        mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

        // Drop nulls
        mapper.setSerializationInclusion(Include.NON_NULL);

        // If props are in JSON that aren't in Java DTO, do not fail.
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Write e.g. Dates as "1975-03-11" instead of timestamp, and instead of array-of-ints [1975, 3, 11].
        // Uses ISO8601 with milliseconds and timezone (if present).
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        // Handle Optional, OptionalLong, OptionalDouble
        mapper.registerModule(new Jdk8Module());

        /*
         * ###### NOTICE! The following part is special for the MatsSocket serialization setup! ######
         */

        //
        // Creating a Mixin for the MatsSocketEnvelopeDto, handling the "msg" field specially:
        //
        // 1) Upon deserialization, deserializes the "msg" field as "pure JSON", i.e. a String containing JSON
        // 2) Upon serialization, serializes the msg field normally (i.e. an instance of Car is JSON serialized),
        // 3) .. UNLESS it is the special type DirectJsonMessage, in which case the JSON is output directly
        //
        mapper.addMixIn(MatsSocketEnvelopeDto.class, MatsSocketEnvelopeDto_Mixin.class);

        return mapper;
    }

    @JsonPropertyOrder({ "t", "smid", "cmid", "x", "ids", "tid", "auth" })
    class MatsSocketEnvelopeDto_Mixin extends MatsSocketEnvelopeWithMetaDto {
        @JsonDeserialize(using = MessageToStringDeserializer.class)
        @JsonSerialize(using = DirectJsonMessageHandlingDeserializer.class)
        public Object msg; // Message, JSON
    }

    /**
     * A {@link MatsSocketEnvelopeWithMetaDto} will be <i>Deserialized</i> (made into object) with the "msg" field
     * directly to the JSON that is present there (i.e. a String, containing JSON), using this class. However, upon
     * <i>serialization</i>, any object there will be serialized to a JSON String (UNLESS it is a {@link DirectJson}, in
     * which case its value is copied in verbatim). The rationale is that upon reception, we do not (yet) know which
     * type (DTO class) this message has, which will be resolved later - and then this JSON String will be deserialized
     * into that specific DTO class.
     */
    class MessageToStringDeserializer extends JsonDeserializer<Object> {
        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            // TODO / OPTIMIZE: Find faster way to get as String, avoiding tons of JsonNode objects.
            // TODO: Trick must be to just consume from the START_OBJECT to the /corresponding/ END_OBJECT.
            return p.readValueAsTree().toString();
        }
    }

    /**
     * A {@link MatsSocketEnvelopeWithMetaDto} will be <i>Serialized</i> (made into object) with the "msg" field handled
     * specially: If it is any other class than {@link DirectJson}, default handling ensues (JSON object serialization)
     * - but if it this particular class, it will output the (JSON) String it contains directly.
     */
    class DirectJsonMessageHandlingDeserializer extends JsonSerializer<Object> {
        @Override
        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            // ?: Is it our special magic String-wrapper that will contain direct JSON?
            if (value instanceof DirectJson) {
                // -> Yes, special magic String-wrapper, so dump it directly.
                gen.writeRawValue(((DirectJson) value).getJson());
            }
            else {
                // -> No, not magic, so serialize it normally.
                gen.writeObject(value);
            }
        }
    }

    /**
     * If the {@link MatsSocketEnvelopeWithMetaDto#msg}-field is of this magic type, the String it contains - which then
     * needs to be proper JSON - will be output directly. Otherwise, it will be JSON serialized.
     */
    class DirectJson {
        private final String _json;

        public static DirectJson of(String msg) {
            if (msg == null) {
                return null;
            }
            return new DirectJson(msg);
        }

        private DirectJson(String json) {
            _json = json;
        }

        public String getJson() {
            return _json;
        }
    }
}
