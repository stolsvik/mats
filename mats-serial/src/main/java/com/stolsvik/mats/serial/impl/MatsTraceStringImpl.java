package com.stolsvik.mats.serial.impl;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.stolsvik.mats.serial.MatsTrace;

/**
 * (Concrete class) Represents the protocol that the MATS endpoints (their stages) communicate with. This class is
 * serialized into a JSON structure that constitute the entire protocol (along with the additional byte arrays
 * ("binaries") and strings that can be added to the payload - but these latter elements are an implementation specific
 * feature).
 * <p>
 * The MatsTrace is designed to contain all previous {@link CallImpl}s in a processing, thus helping the debugging for
 * any particular stage immensely: All earlier calls with data and stack frames for this processing is kept in the
 * trace, thus enabling immediate understanding of what lead up to the particular situation.
 * <p>
 * However, for any particular invocation (invoke, request or reply), only the current (last) {@link CallImpl} - along
 * with the stack frames for the same and lower stack depths than the current call - is needed to execute the stage.
 * This makes it possible to use a condensed variant of MatsTrace that only includes the single current
 * {@link CallImpl}, along with the relevant stack frames. This is defined by the {@link KeepMatsTrace} enum.
 * <p>
 * One envisions that for development and the production stabilization phase of the system, the default long form is
 * used, while when the system have performed flawless for a while, one can change it to use the condensed form, thereby
 * shaving some cycles for the serialization and deserialization, but more importantly potentially quite a bit of
 * bandwidth and message processing compared to transfer of the full trace.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public final class MatsTraceStringImpl implements MatsTrace<String>, Cloneable {
    private final String tid; // TraceId

    private String an; // Initializing AppName
    private String av; // Initializing AppVersion
    private String h; // Initializing Host/Node
    private String iid; // Initiator Id
    private long ts; // Initialized @ TimeStamp (Java epoch)
    private String x; // Debug info (free-form..)

    private final KeepMatsTrace kt; // KeepMatsTrace.
    private final boolean np; // NonPersistent.
    private final boolean rt; // Interactive ... as in "Real Time".

    private List<CallImpl> c = new ArrayList<>(); // Calls. Not final due to clone-impl.
    private List<StackState> ss = new ArrayList<>(); // StackStates. Not final due to clone-impl.
    private Map<String, String> tp = new LinkedHashMap<>(); // TraceProps. Not final due to clone-impl.

    public static MatsTrace<String> createNew(String traceId,
                                              KeepMatsTrace keepMatsTrace, boolean nonPersistent, boolean interactive) {
        return new MatsTraceStringImpl(traceId, keepMatsTrace, nonPersistent, interactive);
    }

    public MatsTraceStringImpl setDebugInfo(String initializingAppName, String initializingAppVersion,
            String initializingHost, String initiatorId,
            long initializedTimestamp, String debugInfo) {
        an = initializingAppName;
        av = initializingAppVersion;
        h = initializingHost;
        iid = initiatorId;
        ts = initializedTimestamp;
        x = debugInfo;
        return this;
    }

    // Jackson JSON-lib needs a default constructor, but it can re-set finals.
    private MatsTraceStringImpl() {
        // REMEMBER: These will be set by the deserialization mechanism.
        tid = null;

        kt = KeepMatsTrace.COMPACT;
        np = false;
        rt = false;
    }

    private MatsTraceStringImpl(String traceId, KeepMatsTrace keepMatsTrace, boolean nonPersistent, boolean interactive) {
        this.tid = traceId;

        this.kt = keepMatsTrace;
        this.np = nonPersistent;
        this.rt = interactive;
    }

    // == NOTICE == Serialization and deserialization is an implementation specific feature.

    @Override
    public String getTraceId() {
        return tid;
    }

    @Override
    public String getInitializingAppName() {
        return an;
    }

    @Override
    public String getInitializingAppVersion() {
        return null;
    }

    @Override
    public String getInitializingHost() {
        return h;
    }

    @Override
    public String getInitiatorId() {
        return iid;
    }

    @Override
    public long getInitializedTimestamp() {
        return ts;
    }

    @Override
    public String getDebugInfo() {
        return x;
    }

    @Override
    public KeepMatsTrace getKeepTrace() {
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

    @Override
    public void setTraceProperty(String propertyName, String propertyValue) {
        tp.put(propertyName, propertyValue);
    }

    @Override
    public String getTraceProperty(String propertyName) {
        return tp.get(propertyName);
    }

    @Override
    public MatsTraceStringImpl addRequestCall(String from, String to, String data, List<String> replyStack,
            String replyState,
            String initialState) {
        MatsTraceStringImpl clone = cloneForNewCall();
        dropValuesOnCurrent();
        clone.c.add(new CallImpl(CallType.REQUEST, from, to, data, replyStack));
        // Add the replyState - i.e. the state that is outgoing from the current call, destined for the reply.
        // (The parameter replyStack includes the replyTo stage/endpointId for this call as the first element, subtract
        // this.)
        clone.ss.add(new StackState(replyStack.size() - 1, replyState));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            clone.ss.add(new StackState(replyStack.size(), initialState));
        }
        // Prune the StackStates if KeepMatsTrace says so
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    @Override
    public MatsTraceStringImpl addSendCall(String from, String to, String data, List<String> replyStack,
            String initialState) {
        MatsTraceStringImpl clone = cloneForNewCall();
        dropValuesOnCurrent();
        clone.c.add(new CallImpl(CallType.SEND, from, to, data, replyStack));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            clone.ss.add(new StackState(replyStack.size(), initialState));
        }
        // Prune the StackStates if KeepMatsTrace says so.
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    @Override
    public MatsTraceStringImpl addNextCall(String from, String to, String data, List<String> replyStack, String state) {
        if (state == null) {
            throw new IllegalStateException("When adding next-call, state-data string should not be null.");
        }
        MatsTraceStringImpl clone = cloneForNewCall();
        dropValuesOnCurrent();
        clone.c.add(new CallImpl(CallType.NEXT, from, to, data, replyStack));
        // Add the state meant for the next stage
        clone.ss.add(new StackState(replyStack.size(), state));
        // Prune the StackStates if KeepMatsTrace says so.
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    @Override
    public MatsTraceStringImpl addReplyCall(String from, String to, String data, List<String> replyStack) {
        MatsTraceStringImpl clone = cloneForNewCall();
        dropValuesOnCurrent();
        clone.c.add(new CallImpl(CallType.REPLY, from, to, data, replyStack));
        // Prune the StackStates if KeepMatsTrace says so.
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    /**
     * Should be invoked just before adding the new call, so as to clean out the from and stack on call that after the
     * add will become the previous call.
     */
    private void dropValuesOnCurrent() {
        if (c.size() > 0) {
            getCurrentCall().dropFromAndStack();
            // ?: Are we on COMPACT mode? (Note that this is implicitly also done for MINIMAL - in clone..())
            if (kt == KeepMatsTrace.COMPACT) {
                // -> Yes, COMPACT, so drop data
                getCurrentCall().dropData();
            }
        }
    }

    /**
     * Should be invoked just after adding a new StackState, so we can clean out any stack states that either are
     * higher than we're at now, or multiples for the same height (only the most recent is actually a part of the
     * stack, the rest on the same level are for history).
     */
    private void pruneUnnecessaryStackStates() {
        // ?: Are we in MINIMAL or COMPACT modes?
        if ((kt == KeepMatsTrace.MINIMAL) || (kt == KeepMatsTrace.COMPACT)) {
            // -> Yes, so we'll drop the states we can.
            int currentPruneDepth = getCurrentCall().getStackSize();

            Set<Integer> seen = new HashSet<>();
            // Iterate over all elements from the most recent (which is the last) to the earliest (which is first).
            for (ListIterator<StackState> it = ss.listIterator(ss.size()); it.hasPrevious();) {
                StackState curr = it.previous();
                // ?: Is this at a higher level than current stack height?
                if (curr.getHeight() > currentPruneDepth) {
                    // -> Yes, so won't ever be used.
                    it.remove();
                    seen.add(curr.getHeight());
                    continue;
                }
                // ?: Have we seen this height before?
                if (seen.contains(curr.getHeight())) {
                    // -> Yes, so since we're traversing backwards, we have the most recent from this height.
                    it.remove();
                }
                else {
                    // -> No, so we've seen it now (delete any subsequent).
                    seen.add(curr.getHeight());
                }
            }
        }
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
     * StackState for the earliest stack element at position 0, while the latest (current) at end of list. The
     * above-specified search algorithm still works, as it now will either find the element with the correct stack depth
     * at the end of the list, or it is not there.
     *
     * @param stackDepth
     *            the stack depth to find stack state for - it should be the size of the stack below you. For e.g. a
     *            Terminator, it is 0. The first request adds a stack level, so it resides at stackDepth 1. Etc.
     * @return the state String if found.
     */
    private String getState(int stackDepth) {
        for (int i = ss.size() - 1; i >= 0; i--) {
            StackState stackState = ss.get(i);
            // ?: Have we reached a lower depth than ourselves?
            if (stackDepth > stackState.getHeight()) {
                // -> Yes, we're at a lower depth: The rest can not possibly be meant for us.
                break;
            }
            if (stackDepth == stackState.getHeight()) {
                return stackState.getState();
            }
        }
        // Did not find any stack state for us.
        return null;
    }

    protected MatsTraceStringImpl cloneForNewCall() {
        MatsTraceStringImpl cloned;
        try {
            cloned = (MatsTraceStringImpl) super.clone();
            // Call are close to immutable. If we're MINIMALing, then only the last Call will be added.
            cloned.c = (kt == KeepMatsTrace.MINIMAL ? new ArrayList<>() : new ArrayList<>(c));
            // StackStates are immutable.
            cloned.ss = new ArrayList<>(ss);
            // TraceProps are immutable.
            cloned.tp = new LinkedHashMap<>(tp);
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
        private String an; // Calling AppName
        private String av; // Calling AppVersion
        private String h; // Calling Host
        private long ts; // Calling TimeStamp

        private String x; // Debug Info (free-form)

        private final CallType t; // type.
        private String f; // from.
        private final String to; // to.
        private String d; // data.
        private List<String> s; // stack.
        private Integer ss; // stack size if stack is nulled.

        // Jackson JSON-lib needs a default constructor, but it can re-set finals.
        private CallImpl() {
            t = null;
            f = null;
            to = null;
            d = null;
            s = null;
        }

        CallImpl(CallType type, String from, String to, String data, List<String> stack) {
            this.t = type;
            this.f = from;
            this.to = to;
            this.d = data;
            this.s = stack;
        }

        public CallImpl setDebugInfo(String callingAppName, String callingAppVersion, String callingHost,
                long calledTimestamp, String debugInfo) {
            an = callingAppName;
            av = callingAppVersion;
            h = callingHost;
            ts = calledTimestamp;
            x = debugInfo;
            return this;
        }

        /**
         * Nulls the "from" and "stack" fields.
         */
        private void dropFromAndStack() {
            f = null;
            ss = s.size();
            s = null;
        }

        /**
         * Nulls the "data" field.
         */
        private void dropData() {
            d = null;
        }

        @Override
        public String getCallingAppName() {
            return an;
        }

        @Override
        public String getCallingAppVersion() {
            return av;
        }

        public String getCallingHost() {
            return h;
        }

        public long getCalledTimestamp() {
            return ts;
        }

        public String getDebugInfo() {
            return x;
        }

        @Override
        public CallType getType() {
            return t;
        }

        @Override
        public String getFrom() {
            if (f == null) {
                return "-nulled-";
            }
            return f;
        }

        @Override
        public String getTo() {
            return to;
        }

        @Override
        public String getData() {
            return d;
        }

        /**
         * @return a COPY of the stack.
         */
        @Override
        public List<String> getStack() {
            if (s == null) {
                new ArrayList<>(Collections.nCopies(getStackSize(), "-nulled-"));
            }
            return new ArrayList<>(s);
        }

        @Override
        public int getStackSize() {
            return (s != null ? s.size() : ss);
        }

        private String indent() {
            return new String(new char[getStackSize()]).replace("\0", ": ");
        }

        private String fromStackData() {
            return "#from:" + (an != null ? an : "") + (av != null ? "[" + av + "]" : "")
                    + (h != null ? "@" + h : "") + (f != null ? ':' + f : "")
                    + (s != null ? ", #stack:" + s : "")
                    + (d != null ? ", #data:" + d : "");
        }

        @Override
        public String toString() {
            return indent()
                    + t
                    + (ts != 0 ? " " + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), TimeZone.getDefault().toZoneId())) + " -"
                            : "")
                    + " #to:" + to
                    + ", " + fromStackData();
        }

        private String spaces(int length) {
            return new String(new char[length]).replace("\0", " ");
        }

        public String toStringFromMatsTrace(long startTimestamp, int maxStackSize, int maxToStageIdLength) {
            String toType = (ts != 0 ? String.format("%4d", (ts - startTimestamp)) + "ms " : " - ")
                    + indent()
                    + t;
            int numMaxIncludingCallType = 14 + maxStackSize * 2;
            String toTo = toType
                    + spaces(numMaxIncludingCallType - toType.length())
                    + " #to:" + to;
            int numMaxIncludingToStageId = 7 + numMaxIncludingCallType + maxToStageIdLength;
            return toTo
                    + spaces(numMaxIncludingToStageId - toTo.length())
                    + fromStackData();
        }
    }

    public static class StackState {
        private final int h; // depth.
        private final String s; // state.

        // Jackson JSON-lib needs a default constructor, but it can re-set finals.
        private StackState() {
            h = 0;
            s = null;
        }

        public StackState(int height, String state) {
            this.h = height;
            this.s = state;
        }

        public int getHeight() {
            return h;
        }

        public String getState() {
            return s;
        }

        @Override
        public String toString() {
            return "height=" + h + ", state=" + s;
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("MatsTrace : ")
                .append(getCurrentCall().getType())
                .append(" #to:").append(getCurrentCall().getTo())
                .append("  [traceId=").append(tid)
                .append("]  KeepMatsTrace:").append(kt).append('\n');
        buf.append(" call#:\n");
        buf.append("    0    --- [Initiator]");
        if (an != null) {
            buf.append(" @").append(an);
        }
        if (av != null) {
            buf.append('[').append(av).append(']');
        }
        if (h != null) {
            buf.append(" @").append(h);
        }
        if (ts != 0) {
            buf.append(" @");
            DateTimeFormatter.ISO_OFFSET_DATE_TIME.formatTo(
                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), TimeZone.getDefault().toZoneId()), buf);
        }
        if (iid != null) {
            buf.append(" #initiatorId:").append(iid);
        }
        buf.append('\n');
        int maxStackSize = c.stream().mapToInt(CallImpl::getStackSize).max().orElse(0);
        int maxToStageIdLength = c.stream().map(Call<String>::getTo).mapToInt(String::length).max().orElse(0);
        for (int i = 0; i < c.size(); i++) {
            buf.append(String.format("   %2d %s\n", i + 1, c.get(i).toStringFromMatsTrace(ts, maxStackSize,
                    maxToStageIdLength)));
        }
        buf.append(" states:\n");
        for (int i = 0; i < ss.size(); i++) {
            buf.append(String.format("   %2d %s", i, ss.get(i)));
            if (i < ss.size() - 1) {
                buf.append('\n');
            }
        }
        return buf.toString();
    }
}
