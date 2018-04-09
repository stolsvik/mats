package com.stolsvik.mats.serial.json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.serial.MatsTrace;

/**
 * (Concrete class) Represents the protocol that the MATS endpoints (their stages) communicate with. This class is
 * serialized into a JSON structure that constitute the entire protocol (along with the additional byte arrays
 * ("binaries") and strings that can be added to the payload - but these latter elements are an implementation specific
 * feature).
 * <p>
 * The MatsTrace is designed to contain all previous {@link CallImpl}s in a processing, thus helping the debugging for any
 * particular stage immensely: All earlier calls with data and stack frames for this processing is kept in the trace,
 * thus enabling immediate understanding of what lead up to the particular situation.
 * <p>
 * However, for any particular invocation (invoke, request or reply), only the current (last) {@link CallImpl} - along with
 * the stack frames for the same and lower stack depths than the current call - is needed to execute the stage. This
 * makes it possible to use a condensed variant of MatsTrace that only includes the single current {@link CallImpl}, along
 * with the relevant stack frames.
 * <p>
 * One envisions that for development and the production stabilization phase of the system, the default long form is
 * used, while when the system have performed flawless for a while, one can change it to use the condensed form, thereby
 * shaving some cycles for the serialization and deserialization, but more importantly potentially quite a bit of
 * bandwidth and message processing compared to transfer of the full trace.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
@SuppressWarnings("PMD")
public final class MatsTraceImpl implements MatsTrace<String>, Cloneable {
    private final String tid;  // TraceId

    private final boolean kt; // KeepTrace.

    private final boolean np; // NonPersistent.

    private final boolean rt; // Interactive ... as in "Real Time".

    private List<CallImpl> c = new ArrayList<>(); // Calls. Not final due to clone-impl.

    private List<StackState> ss = new ArrayList<>(); // StackStates. Not final due to clone-impl.

    private Map<String, String> tp = new LinkedHashMap<>(); // TracePros. Not final due to clone-impl.

    public static MatsTrace<String> createNew(String traceId, boolean keepTrace, boolean nonpersistent,
                                              boolean interactive) {
        return new MatsTraceImpl(traceId, keepTrace, nonpersistent, interactive);
    }

    // Jackson JSON-lib needs a default constructor, but it can re-set finals.
    private MatsTraceImpl() {
        // REMEMBER: These will be set by the deserialization mechanism.
        tid = null;
        kt = false;
        np = false;
        rt = false;
    }

    private MatsTraceImpl(String traceId, boolean keepTrace, boolean nonpersistent, boolean interactive) {
        this.tid = traceId;
        this.kt = keepTrace;
        this.np = nonpersistent;
        this.rt = interactive;
    }

    // == NOTICE == Serialization and deserialization is an implementation specific feature.


    /**
     * @return the TraceId that this {@link MatsTrace} was initiated with - this is set once, at initiation time, and
     *         follows the processing till it terminates. (All log lines will have the traceId set on the MDC.)
     */
    @Override
    public String getTraceId() {
        return tid;
    }

    @Override
    public boolean isKeepTrace() {
        return kt;
    }

    @Override
    public boolean isNonPersistent() {
        return np;
    }

    @Override
    public boolean isInteractive() {
        return rt;
    }

    /**
     * Sets a trace property, refer to {@link ProcessContext#setTraceProperty(String, Object)}. Notice that on the
     * MatsTrace-side, the value must be a String.
     *
     * @param propertyName
     *            the name of the property.
     * @param propertyValue
     *            the value of the property.
     */
    @Override
    public void setTraceProperty(String propertyName, String propertyValue) {
        tp.put(propertyName, propertyValue);
    }

    /**
     * Retrieves a property value set by {@link #setTraceProperty(String, String)}, refer to
     * {@link ProcessContext#getTraceProperty(String, Class)}. Notice that on the MatsTrace-side, the value is a String.
     *
     * @param propertyName
     *            the name of the property to retrieve.
     * @return the value of the property.
     */
    @Override
    public String getTraceProperty(String propertyName) {
        return tp.get(propertyName);
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
    @Override
    public MatsTraceImpl addRequestCall(String from, String to, String data, List<String> replyStack, String replyState,
                                        String initialState) {
        MatsTraceImpl clone = clone();
        clone.c.add(new CallImpl(CallType.REQUEST, from, to, data, replyStack));
        // Add the replyState - i.e. the state that is outgoing from the current call, destined for the reply.
        // (The parameter replyStack includes the replyTo stage/endpointId as first element, subtract this.)
        clone.ss.add(new StackState(replyStack.size() - 1, replyState));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            clone.ss.add(new StackState(replyStack.size(), initialState));
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
    @Override
    public MatsTraceImpl addSendCall(String from, String to, String data, List<String> replyStack, String initialState) {
        MatsTraceImpl clone = clone();
        clone.c.add(new CallImpl(CallType.SEND, from, to, data, replyStack));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            clone.ss.add(new StackState(replyStack.size(), initialState));
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
    @Override
    public MatsTraceImpl addNextCall(String from, String to, String data, List<String> replyStack, String state) {
        if (state == null) {
            throw new IllegalStateException("When adding next-call, state-data string should not be null.");
        }
        MatsTraceImpl clone = clone();
        clone.c.add(new CallImpl(CallType.NEXT, from, to, data, replyStack));
        // Add the state meant for the next stage
        clone.ss.add(new StackState(replyStack.size(), state));
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
     *            for a REPLY call, this would normally be the rest of the list after the first element has been popped
     *            of the stack.
     */
    @Override
    public MatsTraceImpl addReplyCall(String from, String to, String data, List<String> replyStack) {
        MatsTraceImpl clone = clone();
        clone.c.add(new CallImpl(CallType.REPLY, from, to, data, replyStack));
        return clone;
    }

    @Override
    public CallImpl getCurrentCall() {
        // Return last element
        return c.get(c.size() - 1);
    }

    @Override
    public String getCurrentState() {
        // Return the state for the current stack depth (which is the number of stack elements below this).
        return getState(getCurrentCall().getStack().size());
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
        for (int i = ss.size() - 1; i >= 0; i--) {
            StackState stackState = ss.get(i);
            // ?: Have we reached a lower depth than ourselves?
            if (stackDepth > stackState.getDepth()) {
                // -> Yes, we're at a lower depth: The rest can not possibly be meant for us.
                break;
            }
            if (stackDepth == stackState.getDepth()) {
                return stackState.getState();
            }
        }
        // Did not find any stack state for us.
        return null;
    }

    @Override
    protected MatsTraceImpl clone() {
        MatsTraceImpl cloned;
        try {
            cloned = (MatsTraceImpl) super.clone();
            cloned.c = new ArrayList<>(c); // Call are immutable.
            cloned.ss = new ArrayList<>(ss); // StackStaces are immutable.
            cloned.tp = new LinkedHashMap<>(tp); // TraceProps are immutable.
            return cloned;
        }
        catch (CloneNotSupportedException e) {
            throw new AssertionError("Implements Cloneable, so clone() should not throw.", e);
        }
    }

    /**
     * Represents an entry in the {@link MatsTrace}.
     */
    public static class CallImpl implements Call<String> {
        private final CallType t;  // type.

        private final String f;  // from.

        private final String to;  // to.

        private final String d;  // data.

        private final List<String> s;  // stack.

        // Jackson JSON-lib needs a default constructor, but it can re-set finals.
        private CallImpl() {
            this.t = null;
            this.f = null;
            this.to = null;
            this.d = null;
            this.s = null;
        }

        CallImpl(CallType type, String from, String to, String data, List<String> stack) {
            this.t = type;
            this.f = from;
            this.to = to;
            this.d = data;
            this.s = stack;
        }

        public CallType getType() {
            return t;
        }

        public String getFrom() {
            return f;
        }

        public String getTo() {
            return to;
        }

        public String getData() {
            return d;
        }

        /**
         * @return a COPY of the stack.
         */
        public List<String> getStack() {
            return new ArrayList<>(s);
        }

        @Override
        public String toString() {
            return new String(new char[s.size()]).replace("\0", ": ") + t
                    + " #from:" + f + ", #to:" + to
                    + ", #data:" + d + ", #stack:" + s;
        }
    }

    public static class StackState {
        private final int d;   // depth.
        private final String s;   // state.

        // Jackson JSON-lib needs a default constructor, but it can re-set finals.
        private StackState() {
            d = 0;
            s = null;
        }

        public StackState(int depth, String state) {
            this.d = depth;
            this.s = state;
        }

        public int getDepth() {
            return d;
        }

        public String getState() {
            return s;
        }

        @Override
        public String toString() {
            return "depth=" + d + ",state=" + s;
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("MatsTrace [").append("traceId=").append(tid).append("]\n");
        buf.append(" calls:\n");
        buf.append("   i [Initiator]\n");
        for (int i = 0; i < c.size(); i++) {
            buf.append("   ").append(i).append(' ').append(c.get(i)).append("\n");
        }
        buf.append(" states:\n");
        for (int i = 0; i < ss.size(); i++) {
            buf.append("   ").append(i).append(' ').append(ss.get(i)).append("\n");
        }
        return buf.toString();
    }
}
