package com.stolsvik.mats;

import java.util.ArrayList;
import java.util.List;

/**
 * (Concrete class) Represents the protocol that the MATS endpoints and stages communicate with. This class is
 * serialized into a JSON structure that constitute the entire protocol (along with the additional byte arrays
 * ("binaries") and strings that can be added to the payload - but these latter elements are an implementation specific
 * feature).
 * <p>
 * The MatsTrace is designed to contain all previous {@link Call}s in a processing, thus helping the debugging for any
 * particular stage immensely: All earlier calls with data and stack frames for this processing is kept in the trace,
 * thus enabling immediate understanding of what lead up to the particular situation.
 * <p>
 * However, for any particular invocation (invoke, request or reply), only the current (last) {@link Call} - along with
 * the stack frames for the same and lower stack depths than the current call - is needed to execute the stage. This
 * makes it possible to use a condensed variant of MatsTrace that only includes the single current {@link Call}, along
 * with the relevant stack frames.
 * <p>
 * One envisions that for development and the production stabilization phase of the system, the default long form is
 * used, while when the system have performed flawless for a while, one can change it to use the condensed form, thereby
 * shaving some cycles for the serialization and deserialization, but more importantly potentially quite a bit of
 * bandwidth and message processing compared to transfer of the full trace.
 * <p>
 * Serialization and deserialization is left to the implementations, but Jackson JSON databind library is recommended.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class MatsTrace {

    /**
     * Represents the version of the {@link MatsTrace} that the initiator were using, but also points out forward
     * compatible versions, where any other version that is fully encoded in the JSON is appended, delimited by a colon
     * (":"), so that if this trace supports both v1 and v2, then it will read "1:2".
     * <p>
     * (The point of this is to allow for migrations of the protocol, where e.g. for transition from 1 to 2, an
     * intermediate version of MATS will encode and support both v1 and v2. Thus, at some point when all endpoints are
     * migrated to a v2-supporting variant, one can delete the support for v1. This could potentially be configurable.)
     * <p>
     * Set to "1".
     */
    private final String v = "1";

    private final String traceId;

    private final List<Call> calls = new ArrayList<>();

    public static MatsTrace createNew(String traceId) {
        return new MatsTrace(traceId);
    }

    // Jackson JSON-lib needs a default constructor, but it can re-set finals.
    private MatsTrace() {
        traceId = null;
    }

    private MatsTrace(String traceId) {
        this.traceId = traceId;
    }

    // == NOTICE == Serialization and deserialization is an implementation specific feature.

    public enum CallType {
        INVOKE,

        REQUEST,

        REPLY
    }

    public void addRequestCall(String from, String to, String replyTo, String data, List<String> stack) {
        calls.add(new Call(CallType.REQUEST, from, to, replyTo, data, stack));
    }

    public void addInvokeCall(String from, String to, String data, List<String> stack) {
        calls.add(new Call(CallType.INVOKE, from, to, null, data, stack));
    }

    public void addReplyCall(String from, String to, String data, List<String> stack) {
        calls.add(new Call(CallType.REPLY, from, to, null, data, stack));
    }

    public Call getCurrentCall() {
        return calls.get(calls.size() - 1);
    }

    /**
     * Represents an entry in the {@link MatsTrace}.
     */
    public static class Call {
        /**
         * Represents the version of the {@link Call} that the sender were using. The same logic wrt. versioning as for
         * the {@link MatsTrace#v} is employed here.
         * <p>
         * Set to "1".
         */
        private final String v = "1";

        private final CallType type;

        private final String from;

        private final String to;

        private final String replyTo;

        private final String data;

        private final List<String> stack;

        // Jackson JSON-lib needs a default constructor, but it can re-set finals.
        private Call() {
            this.type = null;
            this.from = null;
            this.to = null;
            this.replyTo = null;
            this.data = null;
            this.stack = null;
        }

        Call(CallType type, String from, String to, String replyTo, String data, List<String> stack) {
            this.type = type;
            this.from = from;
            this.to = to;
            this.replyTo = replyTo;
            this.data = data;
            this.stack = stack;
        }

        public String getV() {
            return v;
        }

        public CallType getType() {
            return type;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        public String getReplyTo() {
            return replyTo;
        }

        public String getData() {
            return data;
        }

        public List<String> getStack() {
            return stack;
        }
    }
}
