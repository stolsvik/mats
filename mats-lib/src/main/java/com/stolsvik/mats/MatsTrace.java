package com.stolsvik.mats;

import java.util.ArrayList;
import java.util.List;

/**
 * (Concrete class) Represents the protocol that the MATS endpoints (their stages) communicate with. This class is
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
public class MatsTrace implements Cloneable {
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

    private List<Call> calls = new ArrayList<>(); // Not final due to clone-impl.

    private List<StackState> stackStates = new ArrayList<>(); // Not final due to clone-impl.

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
        REQUEST,

        SEND,

        NEXT,

        REPLY
    }

    /**
     * @return the TraceId that this {@link MatsTrace} was initiated with - this is set once, at initiation time, and
     *         follows the processing till it terminates. (All log lines will have the traceId set on the MDC.)
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * Adds a {@link CallType#REQUEST REQUEST} Call, which is an invocation of a service where one expects a Reply from
     * this service to go to a specified endpoint, typically the next stage in a multi-stage endpoint: Envision a normal
     * invocation of some method that returns a value.
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param to
     *            which endpoint that should get the request.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the requesting
     *            service expects to get.
     * @param replyStack
     *            the stack that the request shall use to decide who to reply to by popping the first element of the
     *            list - that is, the first element of the list is who should get the reply for this request.
     * @param replyState
     *            the state data for the stageId that gets the reply to this request, that is, the state for the stageId
     *            that is at the first element of the replyStack. Most often a JSON representing the State Transfer
     *            Object for the multi-stage endpoint.
     * @param initialState
     *            an optional feature, whereby the state can be set for the initial stage of the requested endpoint.
     *            Same stuff as replyState.
     */
    public MatsTrace addRequestCall(String from, String to, String data, List<String> replyStack, String replyState,
            String initialState) {
        MatsTrace clone = clone();
        clone.calls.add(new Call(CallType.REQUEST, from, to, data, replyStack));
        // Add the replyState - i.e. the state that is outgoing from the current call, destined for the reply.
        // (The parameter replyStack includes the replyTo stage/endpointId as first element, subtract this.)
        clone.stackStates.add(new StackState(replyStack.size() - 1, replyState));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            clone.stackStates.add(new StackState(replyStack.size(), initialState));
        }
        return clone;
    }

    /**
     * Adds a {@link CallType#SEND SEND} Call, meaning a "request" which do not expect a Reply: Envision an invocation
     * of a void-method. Or an invocation of some method that returns the value, but where you invoke it as a
     * void-method (not storing the result, e.g. map.remove("test") returns the removed value, but is often invoked
     * without storing this.).
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param to
     *            which endpoint that should get the message.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the receiving
     *            service expects to get.
     * @param replyStack
     *            for an SEND call, this would normally be an empty list.
     * @param initialState
     *            an optional feature, whereby the state can be set for the initial stage of the requested endpoint.
     */
    public MatsTrace addSendCall(String from, String to, String data, List<String> replyStack, String initialState) {
        MatsTrace clone = clone();
        clone.calls.add(new Call(CallType.SEND, from, to, data, replyStack));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            clone.stackStates.add(new StackState(replyStack.size(), initialState));
        }
        return clone;
    }

    /**
     * Adds a {@link CallType#NEXT NEXT} Call, which is a "sideways call" to the next stage in a multistage service, as
     * opposed to the normal request out to a service expecting a reply. The functionality is functionally identical to
     * {@link #addSendCall(String, String, String, List, String) addSendCall(...)}, but has its own {@link CallType}
     * enum {@link CallType#NEXT value}.
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param to
     *            which endpoint that should get the message - the next stage in a multi-stage service.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the next stage
     *            expects to get.
     * @param replyStack
     *            for an NEXT call, this is the same stack as the current stage has.
     * @param state
     *            the state data for the next stage.
     */
    public MatsTrace addNextCall(String from, String to, String data, List<String> replyStack, String state) {
        MatsTrace clone = clone();
        clone.calls.add(new Call(CallType.NEXT, from, to, data, replyStack));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (state != null) {
            clone.stackStates.add(new StackState(replyStack.size(), state));
        }
        return clone;
    }

    /**
     * Adds a {@link CallType#REPLY REPLY} Call, which happens when a requested service is finished with its processing
     * and have some Reply to return. It then pops the stack (takes the first element of the stack), sets this as the
     * "to" parameter, and provides the rest of the list as the "replyStack" parameter.
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param to
     *            which endpoint that should get the request - for a REPLY Call, this is obtained by popping the first
     *            element of the stack.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the requesting
     *            service expects to get.
     * @param replyStack
     *            for an REPLY call, this would normally be the rest of the list after the first element has been popped
     *            of the stack.
     */
    public MatsTrace addReplyCall(String from, String to, String data, List<String> replyStack) {
        MatsTrace clone = clone();
        clone.calls.add(new Call(CallType.REPLY, from, to, data, replyStack));
        return clone;
    }

    public Call getCurrentCall() {
        // Return last element
        return calls.get(calls.size() - 1);
    }

    public String getCurrentState() {
        // Return the state for the current stack depth (which is the number of stack elements below this).
        return getState(getCurrentCall().stack.size());
    }

    /**
     * Searches in the stack-list from the back (most recent) for the first element that is of the specified stackDepth.
     * If a more shallow stackDepth than the specified is encountered, or the list is exhausted without the stackDepth
     * being found, the search is terminated with null.
     * <p>
     * The point of the stack-list is the same as for the Call list: Monitoring and debugging, by keeping a history of
     * all calls in the processing, along with the states that was present at each call point.
     * <p>
     * If "condensed" is on, the stack-list is - by the condensing algorithm - turned in to a pure stack, with the
     * StackState for the most shallow stack element at position 0, while the deepest (and current) at end of list. The
     * above-specified search algorithm still works, as it now will either find the element with the correct stack depth
     * at the end of the list, or it is not there.
     *
     * @param stackDepth
     *            the stack depth to find stack state for - it should be the size of the stack below you. For e.g. a
     *            Terminator, it is 0. The first request adds a stack element, so it resides at stackDepth 1. Etc.
     * @return the state String if found.
     */
    private String getState(int stackDepth) {
        for (int i = stackStates.size() - 1; i >= 0; i--) {
            StackState stackState = stackStates.get(i);
            // ?: Have we reached a lower depth than ourselves?
            if (stackDepth > stackState.depth) {
                // -> Yes, we're at a lower depth: The rest can not possibly be meant for us.
                break;
            }
            if (stackDepth == stackState.depth) {
                return stackState.state;
            }
        }
        // Did not find any stack state for us.
        return null;
    }

    @Override
    protected MatsTrace clone() {
        MatsTrace cloned;
        try {
            cloned = (MatsTrace) super.clone();
            cloned.calls = new ArrayList<>(calls); // Call are immutable
            cloned.stackStates = new ArrayList<>(stackStates); // StackStaces are immutable
            return cloned;
        }
        catch (CloneNotSupportedException e) {
            throw new AssertionError("Implements Cloneable, so clone() should not throw.", e);
        }
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

        private final String data;

        private final List<String> stack;

        // Jackson JSON-lib needs a default constructor, but it can re-set finals.
        private Call() {
            this.type = null;
            this.from = null;
            this.to = null;
            this.data = null;
            this.stack = null;
        }

        Call(CallType type, String from, String to, String data, List<String> stack) {
            this.type = type;
            this.from = from;
            this.to = to;
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

        public String getData() {
            return data;
        }

        /**
         * @return a COPY of the stack.
         */
        public List<String> getStack() {
            return new ArrayList<>(stack);
        }

        @Override
        public String toString() {
            return new String(new char[stack.size()]).replace("\0", ": ") + type
                    + " #to:" + to + ", #from:" + from
                    + ", #data:" + data + ", #stack:" + stack;
        }
    }

    public static class StackState {
        private final int depth;
        private final String state;

        // Jackson JSON-lib needs a default constructor, but it can re-set finals.
        private StackState() {
            depth = 0;
            state = null;
        }

        public StackState(int depth, String state) {
            this.depth = depth;
            this.state = state;
        }

        public int getDepth() {
            return depth;
        }

        public String getState() {
            return state;
        }

        @Override
        public String toString() {
            return "depth=" + depth + ",state=" + state;
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("MatsTrace [v=").append(v).append(", traceId=").append(traceId).append("]\n");
        buf.append(" calls:\n");
        buf.append("   i [Initiator]\n");
        for (int i = 0; i < calls.size(); i++) {
            buf.append("   ").append(i).append(' ').append(calls.get(i)).append("\n");
        }
        buf.append(" states:\n");
        for (int i = 0; i < stackStates.size(); i++) {
            buf.append("   ").append(i).append(' ').append(stackStates.get(i)).append("\n");
        }
        return buf.toString();
    }

}
