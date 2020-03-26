package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.stolsvik.mats.websocket.MatsSocketServer.MessageType;

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
@JsonPropertyOrder({ "t", "smid", "cmid", "tid" })
class MatsSocketEnvelopeDto {
    MessageType t; // Type

    String clv; // Client Lib and Versions, informative, e.g.
    // "MatsSockLibCsharp,v2.0.3; iOS,v13.2"
    // "MatsSockLibAlternativeJava,v12.3; ASDKAndroid,vKitKat.4.4"
    // Java lib: "MatsSockLibJava,v0.8.9; Java,v11.03:Windows,v2019"
    // browsers/JS: "MatsSocket.js,v0.8.9; User-Agent: <navigator.userAgent string>",
    String an; // AppName
    String av; // AppVersion

    String auth; // Authorization header

    String sid; // SessionId

    Integer rd; // Requested debug info (currently only Client-to-Server)

    String eid; // target MatsSocketEndpointId: Which MatsSocket Endpoint (server/client) this message is for

    String tid; // TraceId
    String smid; // ServerMessageId, messageId from Server.
    String cmid; // ClientMessageId, messageId from Client.
    String x; // PingId - "correlationId" for pings. Small, so as to use little space.

    Long to; // Timeout, in millis, for Client-to-Server Requests.

    String desc; // Description when failure (NACK or others), exception message, multiline, may include stacktrace if
                 // authz.

    @JsonDeserialize(using = MessageToStringDeserializer.class)
    @JsonSerialize(using = DirectJsonMessageHandlingDeserializer.class)
    Object msg; // Message, JSON

    // ::: Debug info

    DebugDto debug; // Debug info object - enabled if requested ('rd') and principal is allowed.

    @Override
    public String toString() {
        return "{" + t
                + (eid == null ? "" : "->" + eid)
                + (tid == null ? "" : " tid:" + tid)
                + (cmid == null ? "" : " cmid:" + cmid)
                + (smid == null ? "" : " smid:" + smid)
                + "}";
    }

    static class DebugDto {
        String d; // Description

        int resd; // Resolved DebugOptions

        // :: Timings and Nodenames
//        Long cmcts; // Client Message Created TimeStamp (when message was created on Client side, Client timestamp)
        Long cmrts; // Client Message Received Timestamp (when message was received on Server side, Server timestamp)
        String cmrnn; // Client Message Received on NodeName (and Mats message is also sent from this)

        Long mmsts; // Mats Message Sent Timestamp (when the message was sent onto Mats MQ fabric, Server timestamp)
        Long mmrrts; // Mats Message Reply Received Timestamp (when the message was received from Mats, Server ts)
        String mmrrnn; // Mats Message Reply Received on NodeName

        Long smcts; // Server Message Created Timestamp (when message was created on Server side)
        String smcnn; // Server Message Created NodeName (when message was created on Server side)

        Long mscts; // Message Sent to Client Timestamp (when the message was replied to Client side, Server timestamp)
        String mscnn; // Message Sent to Client from NodeName (typically same as cmrnn, unless reconnect in between)

        // The log
        List<LogLineDto> l; // Log - this will be appended to if debugging is active.
    }

    static class LogLineDto {
        long ts; // TimeStamp
        String s; // System: "MatsSockets", "Mats", "MS SQL" or similar.
        String hos; // Host OS, e.g. "iOS,v13.2", "Android,vKitKat.4.4", "Chrome,v123:Windows,vXP",
        // "Java,v11.03:Windows,v2019"
        String an; // AppName
        String av; // AppVersion
        String t; // Thread name
        int level; // 0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR
        String m; // Message
        String x; // Exception if any, null otherwise.
        Map<String, String> mdc; // The MDC
    }

    /**
     * A {@link MatsSocketEnvelopeDto} will be <i>Deserialized</i> (made into object) with the "msg" field directly to
     * the JSON that is present there (i.e. a String, containing JSON), using this class. However, upon
     * <i>serialization</i>, any object there will be serialized to a JSON String (UNLESS it is a
     * {@link DirectJsonMessage}, in which case its value is copied in verbatim). The rationale is that upon reception,
     * we do not (yet) know which type (DTO class) this message has, which will be resolved later - and then this JSON
     * String will be deserialized into that specific DTO class.
     */
    private static class MessageToStringDeserializer extends JsonDeserializer<Object> {
        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            // TODO / OPTIMIZE: Find faster way to get as String, avoiding tons of JsonNode objects.
            // TODO: Trick must be to just consume from the START_OBJECT to the /corresponding/ END_OBJECT.
            return p.readValueAsTree().toString();
        }
    }

    /**
     * A {@link MatsSocketEnvelopeDto} will be <i>Serialized</i> (made into object) with the "msg" field handled
     * specially: If it is any other class than {@link DirectJsonMessage}, default handling ensues (JSON object
     * serialization) - but if it this particular class, it will output the (JSON) String it contains directly.
     */
    private static class DirectJsonMessageHandlingDeserializer extends JsonSerializer<Object> {
        @Override
        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            // ?: Is it our special magic String-wrapper that will contain direct JSON?
            if (value instanceof DirectJsonMessage) {
                // -> Yes, special magic String-wrapper, so dump it directly.
                gen.writeRawValue(((DirectJsonMessage) value).getJson());
            }
            else {
                // -> No, not magic, so serialize it normally.
                gen.writeObject(value);
            }
        }
    }

    /**
     * If the {@link MatsSocketEnvelopeDto#msg}-field is of this magic type, the String it contains - which then needs
     * to be proper JSON - will be output directly. Otherwise, it will be JSON serialized.
     */
    static class DirectJsonMessage {
        private final String _json;

        public static DirectJsonMessage of(String msg) {
            if (msg == null) {
                return null;
            }
            return new DirectJsonMessage(msg);
        }

        public DirectJsonMessage(String json) {
            _json = json;
        }

        public String getJson() {
            return _json;
        }
    }
}
