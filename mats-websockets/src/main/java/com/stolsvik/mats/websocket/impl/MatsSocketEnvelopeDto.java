package com.stolsvik.mats.websocket.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * @author Endre Stølsvik 2019-11-28 12:17 - http://stolsvik.com/, endre@stolsvik.com
 */
@JsonPropertyOrder({ "t", "st", "smid", "cmid", "tid" })
class MatsSocketEnvelopeDto {
    String clv; // Client Lib and Versions, informative, e.g.
    // "MatsSockLibCsharp,v2.0.3; iOS,v13.2"
    // "MatsSockLibAlternativeJava,v12.3; ASDKAndroid,vKitKat.4.4"
    // Java lib: "MatsSockLibJava,v0.8.9; Java,v11.03:Windows,v2019"
    // browsers/JS: "MatsSocket.js,v0.8.9; User-Agent: <navigator.userAgent string>",
    String an; // AppName
    String av; // AppVersion

    String auth; // Authorization header

    String tid; // TraceId
    String sid; // SessionId
    String cid; // CorrelationId
    String smid; // ServerMessageId, messageId from Server. String, since JavaScript bad on Long.
    String cmid; // ClientMessageId, messageId from Client - this is for SEND and REQUEST messages.
    String eid; // target MatsSocketEndpointId: Which MatsSocket Endpoint (server/client) this message is for
    String reid; // reply MatsSocketEndpointId: Which MatsSocket Endpoint (client/server) this message is for

    String t; // Type
    String st; // "SubType": AUTH_FAIL:"enum", EXCEPTION:Classname, MSGERROR:"enum"
    String desc; // Description of "st" of failure, exception message, multiline, may include stacktrace if authz.
    String inMsg; // On MSGERROR: Incoming Message, BASE64 encoded.

    @JsonDeserialize(using = MessageToStringDeserializer.class)
    Object msg; // Message, JSON

    // ::: Debug info

    // :: Timings and Nodenames
    Long cmcts; // Client Message Created TimeStamp (when message was created on Client side, Client timestamp)
    Long cmrts; // Client Message Received Timestamp (when message was received on Server side, Server timestamp)
    String cmrnn; // Client Message Received on NodeName (and Mats message is also sent from this)
    Long mmsts; // Mats Message Sent Timestamp (when the message was sent onto Mats MQ fabric, Server timestamp)
    Long mmrrts; // Mats Message Reply Received Timestamp (when the message was received from Mats, Server
    // timestamp)
    String mmrrnn; // Mats Message Reply Received on NodeName
    Long mscts; // Message Sent to Client Timestamp (when the message was replied to Client side, Server timestamp)
    String mscnn; // Message Sent to Client from NodeName (typically same as cmrnn, unless reconnect in between)

    DebugDto dbg; // Debug info object - enabled if requested and principal is allowed. (Not Yet Impl)

    @Override
    public String toString() {
        return "[" + t + (st == null ? "" : ":" + st) + "]->"
                + eid + (reid == null ? "" : ",reid:" + reid)
                + ",tid:" + tid + ",cid:" + cid;
    }

    static class DebugDto {
        String d; // Description
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
     * A {@link MatsSocketEnvelopeDto} will be <i>deserialized</i> (made into object) with the "msg" field directly to
     * the JSON that is present there (i.e. a String, containing JSON), using this class. However, upon
     * <i>serialization</i>, any object there will be serialized to a JSON String. The rationale is that upon reception,
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
}
