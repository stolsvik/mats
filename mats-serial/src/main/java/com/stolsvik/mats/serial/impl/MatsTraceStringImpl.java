package com.stolsvik.mats.serial.impl;

import java.nio.charset.StandardCharsets;
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
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call.CallType;
import com.stolsvik.mats.serial.MatsTrace.Call.Channel;
import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;

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
 * One envisions that for development and the production stabilization phase of the system, the long form is used, while
 * when the system have performed flawless for a while, one can change it to use the condensed form, thereby shaving
 * some cycles for the serialization and deserialization, but more importantly potentially quite a bit of bandwidth and
 * message processing compared to transfer of the full trace.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public final class MatsTraceStringImpl implements MatsTrace<String>, Cloneable {

    private final String id; // "Flow Id", system-def Id for this call flow (as oppose to traceId, which is user def.)
    private final String tid; // TraceId, user-def Id for this call flow.

    private Long tidh; // For future OpenTracing support: 16-byte TraceId HIGH
    private Long tidl; // For future OpenTracing support: 16-byte TraceId LOW
    private Long sid; // For future OpenTracing support: Override SpanId for root
    private Long pid; // For future OpenTracing support: ParentId
    private Byte f;  // For future OpenTracing support: Flags

    private int d; // For future Debug options, issue #79

    private String an; // Initializing AppName
    private String av; // Initializing AppVersion
    private String h; // Initializing Host/Node
    private String iid; // Initiator Id, "from" on initiation
    private long ts; // Initialized @ TimeStamp (Java epoch)
    private String x; // Debug info (free-form..)

    private final KeepMatsTrace kt; // KeepMatsTrace.
    private final Boolean np; // NonPersistent.
    private final Boolean ia; // Interactive.
    private final Long tl; // Time-To-Live, null if 0, where 0 means "forever".
    private final Boolean na; // NoAudit.

    private int cn; // Call Number. Not final due to clone-impl.

    private List<CallImpl> c = new ArrayList<>(); // Calls, "Call Flow". Not final due to clone-impl.
    private List<StackStateImpl> ss = new ArrayList<>(); // StackStates. Not final due to clone-impl.
    private Map<String, String> tp = new LinkedHashMap<>(); // TraceProps. Not final due to clone-impl.

    /**
     * @deprecated Use {@link #createNew(String, String, KeepMatsTrace, boolean, boolean, long, boolean)}.
     */
    @Deprecated
    public static MatsTrace<String> createNew(String traceId,
            KeepMatsTrace keepMatsTrace, boolean nonPersistent, boolean interactive) {
        // Since it was called without a FlowId, we generate one here.
        Random random = ThreadLocalRandom.current();
        String flowId = "mid_" + Long.toUnsignedString(System.currentTimeMillis(), 36)
                + "_" + Long.toUnsignedString(random.nextLong(), 36)
                + Long.toUnsignedString(random.nextLong(), 36);

        return new MatsTraceStringImpl(traceId, flowId, keepMatsTrace, nonPersistent, interactive, 0, false, 0);
    }

    /**
     * Creates a new {@link MatsTrace}. Must add a {@link Call} before sending.
     * 
     * @param traceId
     *            the user-defined hopefully-unique id for this call flow.
     * @param flowId
     *            the system-defined pretty-much-(for <i>all</i> purposes)-guaranteed-unique id for this call flow.
     * @param keepMatsTrace
     *            the level of "trace keeping".
     * @param nonPersistent
     *            if the messages in this flow should be non-persistent
     * @param interactive
     *            if the messages in this flow is of "interactive" priority.
     * @param ttlMillis
     *            the number of milliseconds the message should live before being time out. 0 means "forever", and is
     *            the default.
     * @param noAudit
     *            hint to the underlying implementation, or to any monitoring/auditing tooling on the Message Broker,
     *            that it does not make much value in auditing this message flow, typically because it is just a
     *            "getter" of information to show to some user, or a health-check validating that some service is up and
     *            answers in a timely fashion.
     * @return the newly created {@link MatsTrace}.
     */
    public static MatsTrace<String> createNew(String traceId, String flowId,
            KeepMatsTrace keepMatsTrace, boolean nonPersistent, boolean interactive, long ttlMillis, boolean noAudit) {
        return new MatsTraceStringImpl(traceId, flowId, keepMatsTrace, nonPersistent, interactive, ttlMillis, noAudit,
                0);
    }

    public MatsTraceStringImpl withDebugInfo(String initializingAppName, String initializingAppVersion,
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

    // TODO: POTENTIAL withOpenTracingTraceId() and withOpenTracingSpanId()..

    // Jackson JSON-lib needs a no-args constructor, but it can re-set finals.
    private MatsTraceStringImpl() {
        // REMEMBER: These will be set by the deserialization mechanism.
        tid = null;
        id = null;

        kt = KeepMatsTrace.COMPACT;
        np = null;
        ia = null;
        tl = null;
        na = null;
    }

    private MatsTraceStringImpl(String traceId, String flowId, KeepMatsTrace keepMatsTrace, boolean nonPersistent,
            boolean interactive, long ttlMillis, boolean noAudit, int callNumber) {
        this.tid = traceId;
        this.id = flowId;

        this.kt = keepMatsTrace;
        this.np = nonPersistent ? Boolean.TRUE : null;
        this.ia = interactive ? Boolean.TRUE : null;
        this.tl = ttlMillis > 0 ? ttlMillis : null;
        this.na = noAudit ? Boolean.TRUE : null;
        this.cn = callNumber;
    }

    // == NOTICE == Serialization and deserialization is an implementation specific feature.

    @Override
    public String getTraceId() {
        return tid;
    }

    @Override
    public String getFlowId() {
        return id;
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

    /**
     * @return the "from" of the initiation.
     */
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
        return np == null ? Boolean.FALSE : np;
    }

    @Override
    public boolean isInteractive() {
        return ia == null ? Boolean.FALSE : ia;
    }

    @Override
    public long getTimeToLive() {
        return tl != null ? tl : 0;
    }

    @Override
    public boolean isNoAudit() {
        return na == null ? Boolean.FALSE : na;
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
    public Set<String> getTracePropertyKeys() {
        return tp.keySet();
    }

    @Override
    public MatsTraceStringImpl addRequestCall(String from,
            String to, MessagingModel toMessagingModel,
            String replyTo, MessagingModel replyToMessagingModel,
            String data, String replyState, String initialState) {
        MatsTraceStringImpl clone = cloneForNewCall();
        // Get the stack from /this/
        List<ChannelWithSpan> newCallReplyStack = getCurrentStack();
        // Add the replyState - i.e. the state that is outgoing from the current call, destined for the reply.
        // NOTE: This must be added BEFORE we add to the newCallReplyStack, since it is targeted to the stack frame
        // below us!
        clone.ss.add(new StackStateImpl(newCallReplyStack.size(), replyState));
        // Add the stageId to replyTo to the stack
        newCallReplyStack.add(ChannelWithSpan.newWithRandomSpanId(replyTo, replyToMessagingModel));
        // Prune the data and stack from current call if KeepMatsTrace says so.
        clone.dropValuesOnCurrentCallIfAny();
        // Add the new Call
        clone.c.add(new CallImpl(CallType.REQUEST, from, new ChannelImpl(to, toMessagingModel), data,
                newCallReplyStack));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            // The stack is now one height higher, since we added the "replyTo" to it.
            clone.ss.add(new StackStateImpl(newCallReplyStack.size(), initialState));
        }
        // Prune the StackStates if KeepMatsTrace says so
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    @Override
    public MatsTraceStringImpl addSendCall(String from, String to, MessagingModel toMessagingModel,
            String data, String initialState) {
        MatsTraceStringImpl clone = cloneForNewCall();
        // For a send/next call, the stack does not change.
        List<ChannelWithSpan> newCallReplyStack = getCurrentStack();
        // Prune the data and stack from current call if KeepMatsTrace says so.
        clone.dropValuesOnCurrentCallIfAny();
        // Add the new Call
        clone.c.add(new CallImpl(CallType.SEND, from, new ChannelImpl(to, toMessagingModel), data,
                newCallReplyStack));
        // Add any state meant for the initial stage ("stage0") of the "to" endpointId.
        if (initialState != null) {
            clone.ss.add(new StackStateImpl(newCallReplyStack.size(), initialState));
        }
        // Prune the StackStates if KeepMatsTrace says so.
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    @Override
    public MatsTraceStringImpl addNextCall(String from, String to, String data, String state) {
        if (state == null) {
            throw new IllegalStateException("When adding next-call, state-data string should not be null.");
        }
        MatsTraceStringImpl clone = cloneForNewCall();
        // For a send/next call, the stack does not change.
        List<ChannelWithSpan> newCallReplyStack = getCurrentStack();
        // Prune the data and stack from current call if KeepMatsTrace says so.
        clone.dropValuesOnCurrentCallIfAny();
        // Add the new Call.
        clone.c.add(new CallImpl(CallType.NEXT, from, new ChannelImpl(to, MessagingModel.QUEUE), data,
                newCallReplyStack));
        // Add the state meant for the next stage
        clone.ss.add(new StackStateImpl(newCallReplyStack.size(), state));
        // Prune the StackStates if KeepMatsTrace says so.
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    @Override
    public MatsTraceStringImpl addReplyCall(String from, String data) {
        List<ChannelWithSpan> newCallReplyStack = getCurrentStack();
        if (newCallReplyStack.size() == 0) {
            throw new IllegalStateException("Trying to add Reply Call when there is no stack."
                    + " (Implementation note: You need to check the getCurrentCall().getStackHeight() before trying to"
                    + " do a reply - if it is zero, then just drop the reply instead.)");
        }
        MatsTraceStringImpl clone = cloneForNewCall();
        // Prune the data and stack from current call if KeepMatsTrace says so.
        clone.dropValuesOnCurrentCallIfAny();
        // Pop the last element off the stack, since this is where we'll reply to, and the rest is the new stack.
        ChannelImpl to = newCallReplyStack.remove(newCallReplyStack.size() - 1);
        // Add the new Call, adding the ReplyForSpanId.
        CallImpl replyCall = new CallImpl(CallType.REPLY, from, to, data, newCallReplyStack)
                .setReplyForSpanId(getCurrentSpanId());
        clone.c.add(replyCall);
        // Prune the StackStates if KeepMatsTrace says so.
        clone.pruneUnnecessaryStackStates();
        return clone;
    }

    // TODO: POTENTIAL setSpanIdOnCurrentStack(..)

    @Override
    public long getCurrentSpanId() {
        // ?: Do we have a CurrentCall?
        CallImpl currentCall = getCurrentCall();
        if (currentCall == null) {
            // -> No, so then we derive the SpanId from the FlowId
            return getRootSpanId();
        }
        // E-> Yes, we have a CurrentCall
        List<ChannelWithSpan> stack = currentCall.s;
        // ?: Is there any stack?
        if (stack.isEmpty()) {
            // -> No, no stack, so we're at initiator/terminator level - again derive SpanId from FlowId
            return getRootSpanId();
        }
        // E-> Yes, we have a CurrentCall with a Stack > 0 elements.
        return stack.get(stack.size() - 1).getSpanId();
    }

    private long getRootSpanId() {
        // TODO: Remove this hack in 2020.
        if (getFlowId() == null) {
            return 0;
        }
        return fnv1a_64(getFlowId().getBytes(StandardCharsets.UTF_8));
    }

    private List<Long> getSpanIdStack() {
        ArrayList<Long> spanIds = new ArrayList<>();
        spanIds.add(getRootSpanId());
        CallImpl currentCall = getCurrentCall();
        // ?: Did we have a CurrentCall?
        if (currentCall != null) {
            // -> We have a CurrentCall, add the stack of SpanIds.
            for (ChannelWithSpan cws : currentCall.s) {
                spanIds.add(cws.sid);
            }
        }
        return spanIds;
    }

    private static final long FNV1A_64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV1A_64_PRIME = 0x100000001b3L;

    /**
     * Fowler–Noll–Vo hash function, https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
     */
    private static long fnv1a_64(final byte[] k) {
        long rv = FNV1A_64_OFFSET_BASIS;
        for (byte b : k) {
            rv ^= b;
            rv *= FNV1A_64_PRIME;
        }
        return rv;
    }

    /**
     * @return a COPY of the current stack.
     */
    private List<ChannelWithSpan> getCurrentStack() {
        CallImpl currentCall = getCurrentCall();
        // ?: Do we have a Current Call?
        if (currentCall != null) {
            // -> Yes, we have a current call, return its stack
            return currentCall.getStack_internal(); // This is a copy.
        }
        // E-> No, no Current call, thus we by definition have an empty stack.
        return new ArrayList<>();
    }

    /**
     * Should be invoked just before adding the new call to the cloneForNewCall()'ed MatsTrace, so as to clean out the
     * 'from' and Stack (and data if COMPACT) on the CurrentCall which after the add will become the <i>previous</i>
     * call.
     */
    private void dropValuesOnCurrentCallIfAny() {
        if (c.size() > 0) {
            getCurrentCall().dropFromAndStack();
            // ?: Are we on COMPACT mode? (Note that this is implicitly also done for MINIMAL - in cloneForNewCall() -
            // since all calls are dropped in MINIMAL!)
            if (kt == KeepMatsTrace.COMPACT) {
                // -> Yes, COMPACT, so drop data
                getCurrentCall().dropData();
            }
        }
    }

    /**
     * Should be invoked just after adding a new StackState, so we can clean out any stack states that either are higher
     * than we're at now, or multiples for the same height (only the most recent is actually a part of the stack, the
     * rest on the same level are for history).
     */
    private void pruneUnnecessaryStackStates() {
        // ?: Are we in MINIMAL or COMPACT modes?
        if ((kt == KeepMatsTrace.MINIMAL) || (kt == KeepMatsTrace.COMPACT)) {
            // -> Yes, so we'll drop the states we can.
            int currentPruneDepth = getCurrentCall().getStackHeight();
            pruneUnnecessaryStackStates(ss, currentPruneDepth);
        }
    }

    private static void pruneUnnecessaryStackStates(List<StackStateImpl> stackStates, int currentPruneDepth) {
        Set<Integer> seen = new HashSet<>();
        // Iterate over all elements backwards, from the most recent (which is the last) to the oldest (which is first).
        for (ListIterator<StackStateImpl> it = stackStates.listIterator(stackStates.size()); it.hasPrevious();) {
            StackStateImpl curr = it.previous();
            // ?: Is this at a higher level than current stack height?
            if (curr.getHeight() > currentPruneDepth) {
                // -> Yes, so won't ever be used.
                it.remove();
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

    @Override
    public CallImpl getCurrentCall() {
        // ?: No calls?
        if (c.size() == 0) {
            // -> No calls, so return null.
            return null;
        }
        // Return last element
        return c.get(c.size() - 1);
    }

    @Override
    public int getCallNumber() {
        return cn;
    }

    @Override
    public List<Call<String>> getCallFlow() {
        return new ArrayList<>(c);
    }

    @Override
    public String getCurrentState() {
        // Return the state for the current stack depth (which is the number of stack elements below this).
        return getState(getCurrentCall().getStackHeight());
    }

    @Override
    public List<StackState<String>> getStateFlow() {
        return new ArrayList<>(ss);
    }

    @Override
    public List<StackState<String>> getStateStack() {
        List<StackStateImpl> stackStates = new ArrayList<>(ss);
        pruneUnnecessaryStackStates(stackStates, getCurrentCall().getStackHeight());
        return new ArrayList<>(stackStates);
    }

    /**
     * Searches in the stack-list from the back (most recent) for the first element that is of the specified stackDepth.
     * If a more shallow stackDepth than the specified is encountered, or the list is exhausted without the stackDepth
     * being found, the search is terminated with null.
     *
     * @param stackDepth
     *            the stack depth to find stack state for - it should be the size of the stack below you. For e.g. a
     *            Terminator, it is 0. The first request adds a stack level, so it resides at stackDepth 1. Etc.
     * @return the state String if found, <code>null</code> otherwise (as is typical when entering "stage0").
     */
    private String getState(int stackDepth) {
        for (int i = ss.size() - 1; i >= 0; i--) {
            StackStateImpl stackState = ss.get(i);
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

    /**
     * Takes into account the KeepMatsTrace value.
     */
    protected MatsTraceStringImpl cloneForNewCall() {
        try {
            MatsTraceStringImpl cloned = (MatsTraceStringImpl) super.clone();
            // Calls are not immutable (a Call's stack and data may be nulled due to KeepMatsTrace value)
            // ?: Are we using MINIMAL?
            if (kt == KeepMatsTrace.MINIMAL) {
                // -> Yes, MINIMAL, so we will literally just have the sole "NewCall" in the trace.
                cloned.c = new ArrayList<>(1);
            }
            else {
                // -> No, not MINIMAL (i.e. FULL or COMPACT), so clone up the Calls.
                cloned.c = new ArrayList<>(c.size());
                // Clone all the calls.
                for (CallImpl call : c) {
                    cloned.c.add(call.clone());
                }
            }
            // StackStates are immutable.
            cloned.ss = new ArrayList<>(ss);
            // TraceProps are immutable.
            cloned.tp = new LinkedHashMap<>(tp);
            // Increase CallNumber
            cloned.cn = this.cn + 1;
            return cloned;
        }
        catch (CloneNotSupportedException e) {
            throw new AssertionError("Implements Cloneable, so clone() should not throw.", e);
        }
    }

    /**
     * Represents an entry in the {@link MatsTrace}.
     */
    public static class CallImpl implements Call<String>, Cloneable {
        private String an; // Calling AppName
        private String av; // Calling AppVersion
        private String h; // Calling Host
        private long ts; // Calling TimeStamp
        private String id; // MatsMessageId.

        private String x; // Debug Info (free-form)

        private final CallType t; // type.
        private String f; // from, may be nulled.
        private final ChannelImpl to; // to.
        private String d; // data, may be nulled.
        private List<ChannelWithSpan> s; // stack of reply channels, may be nulled, in which case 'ss' is set.
        private Integer ss; // stack size if stack is nulled.

        private Long rid; // Reply-From-SpanId

        // Jackson JSON-lib needs a no-args constructor, but it can re-set finals.
        private CallImpl() {
            t = null;
            to = null;
        }

        CallImpl(CallType type, String from, ChannelImpl to, String data,
                List<ChannelWithSpan> stack) {
            this.t = type;
            this.f = from;
            this.to = to;
            this.d = data;
            this.s = stack;
        }

        /**
         * Deprecated. Sets MatsMessageId to a random String.
         */
        public CallImpl setDebugInfo(String callingAppName, String callingAppVersion, String callingHost,
                long calledTimestamp, String debugInfo) {
            an = callingAppName;
            av = callingAppVersion;
            h = callingHost;
            ts = calledTimestamp;
            x = debugInfo;

            // Since it was called without a MatsMessageId, we generate one here.
            Random random = new Random();
            id = "mid_" + Long.toUnsignedString(System.currentTimeMillis(), 36)
                    + "_" + Long.toUnsignedString(random.nextLong(), 36)
                    + Long.toUnsignedString(random.nextLong(), 36);

            return this;
        }

        public CallImpl setDebugInfo(String callingAppName, String callingAppVersion, String callingHost,
                long calledTimestamp, String matsMessageId, String debugInfo) {
            an = callingAppName;
            av = callingAppVersion;
            h = callingHost;
            ts = calledTimestamp;
            id = matsMessageId;
            x = debugInfo;
            return this;
        }

        public CallImpl setReplyForSpanId(long replyForSpanId) {
            rid = replyForSpanId;
            return this;
        }

        /**
         * Nulls the "from" and "stack" fields.
         */
        void dropFromAndStack() {
            f = null;
            ss = s.size();
            s = null;
        }

        /**
         * Nulls the "data" field.
         */
        void dropData() {
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

        @Override
        public String getCallingHost() {
            return h;
        }

        @Override
        public long getCalledTimestamp() {
            return ts;
        }

        @Override
        public String getMatsMessageId() {
            return id;
        }

        @Override
        public String getDebugInfo() {
            return x;
        }

        @Override
        public CallType getCallType() {
            return t;
        }

        @Override
        public long getReplyFromSpanId() {
            if (getCallType() != CallType.REPLY) {
                throw new IllegalStateException("Type of this call is not REPLY, so you cannot ask for"
                        + " ReplyFromSpanId.");
            }
            // TODO: REMOVE THIS HACK IN 2020
            if (rid == null) {
                return 0;
            }
            return rid;
        }

        @Override
        public String getFrom() {
            if (f == null) {
                return "-nulled-";
            }
            return f;
        }

        @Override
        public Channel getTo() {
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
        public List<Channel> getStack() {
            // Dirty, absurd stuff. Please give me a pull request if you know a better way! ;) -endre.
            @SuppressWarnings("unchecked")
            List<Channel> ret = (List<Channel>) (List) getStack_internal();
            return ret;
        }

        /**
         * @return a COPY of the stack.
         */
        List<ChannelWithSpan> getStack_internal() {
            // ?: Has the stack been nulled (to conserve space) due to not being Current Call?
            if (s == null) {
                // -> Yes, nulled, so return a list of correct size where all elements are the string "-nulled-".
                return new ArrayList<>(Collections.nCopies(getStackHeight(),
                        new ChannelWithSpan("-nulled-", null, 0)));
            }
            // E-> No, not nulled (thus Current Call), so return the stack.
            return new ArrayList<>(s);
        }

        @Override
        public int getStackHeight() {
            return (s != null ? s.size() : ss);
        }

        private String indent() {
            return new String(new char[getStackHeight()]).replace("\0", ": ");
        }

        private String fromStackData(boolean printNullData) {
            return "#from:" + (an != null ? an : "") + (av != null ? "[" + av + "]" : "")
                    + (h != null ? "@" + h : "") + (id != null ? ':' + id : "")
                    + (((d != null) || printNullData) ? ", #data:" + d : "");
        }

        @Override
        public String toString() {
            return indent()
                    + t
                    + (ts != 0 ? " " + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), TimeZone.getDefault().toZoneId())) + " -"
                            : "")
                    + " #to:" + to
                    + ", " + fromStackData(false);
        }

        public String toStringFromMatsTrace(long startTimestamp, int maxStackSize, int maxToStageIdLength,
                boolean printNulLData) {
            String toType = (ts != 0 ? String.format("%4d", (ts - startTimestamp)) + "ms " : " - ") + indent() + t;
            int numMaxIncludingCallType = 14 + maxStackSize * 2;
            int numSpacesTo = Math.max(0, numMaxIncludingCallType - toType.length());
            String toTo = toType + spaces(numSpacesTo) + " #to:" + to;
            int numSpacesStack = Math.max(1, 7 + numMaxIncludingCallType + maxToStageIdLength - toTo.length());
            return toTo + spaces(numSpacesStack) + fromStackData(printNulLData);
        }

        protected CallImpl clone() {
            try {
                CallImpl cloned = (CallImpl) super.clone();
                // Channels are immutable.
                cloned.s = (s == null ? null : new ArrayList<>(s));
                return cloned;
            }
            catch (CloneNotSupportedException e) {
                throw new AssertionError("Implements Cloneable, so clone() should not throw.", e);
            }
        }
    }

    private static class ChannelImpl implements Channel {
        private final String i;
        private final MessagingModel m;

        // Jackson JSON-lib needs a no-args constructor, but it can re-set finals.
        private ChannelImpl() {
            i = null;
            m = null;
        }

        public ChannelImpl(String i, MessagingModel m) {
            this.i = i;
            this.m = m;
        }

        @Override
        public String getId() {
            return i;
        }

        @Override
        public MessagingModel getMessagingModel() {
            return m;
        }

        @Override
        public String toString() {
            String model;
            switch (m) {
                case QUEUE:
                    model = "Q";
                    break;
                case TOPIC:
                    model = "T";
                    break;
                default:
                    model = m.toString();
            }
            return "[" + model + "]" + i;
        }
    }

    private static String spaces(int length) {
        return new String(new char[length]).replace("\0", " ");
    }

    /**
     * We're hitching the SpanIds onto the ReplyTo Stack, as they have the same stack semantics. However, do note that
     * the Channel-stack and the SpanId-stack are "offset" wrt. to what they refer to:
     * <ul>
     * <li>ReplyTo Stack: The topmost ChannelWithSpan in the stack is what this CurrentCall <i>shall reply to</i>, if it
     * so desires - i.e. it references the the frame <i>below</i> it in the stack, its <i>parent</i>.</li>
     * <li>SpanId Stack: The topmost ChannelWithSpan in the stack carries the SpanId that <i>this</i> Call processes
     * within - i.e. it refers to <i>this</i> frame in the stack.</li>
     * </ul>
     * However, when correlating with how OpenTracing and friends refer to SpanIds, these are always created by the
     * parent - which is also the case here: When a new REQUEST Call is made, this creates a new SpanId (which is kept
     * with the Channel that should be replied to, i.e. in this class) - and then the Call is being sent (inside the
     * MatsTrace). Then, when the REPLY Call is being created from the requested service, this SpanId is propagated back
     * in the Call, accessible via the {@link Call#getReplyFromSpanId()} method. Thus, the SpanId is both created, and
     * then processed again upon receiving the REPLY, by the parent stackframe - and viewed like this, the SpanId
     * ('sid') thus actually resides on the correct stackframe.
     */
    private static class ChannelWithSpan extends ChannelImpl {
        private final long sid; // SpanId

        // Jackson JSON-lib needs a no-args constructor, but it can re-set finals.
        public ChannelWithSpan() {
            super();
            sid = 0;
        }

        public ChannelWithSpan(String i, MessagingModel m, long sid) {
            super(i, m);
            this.sid = sid;
        }

        public static ChannelWithSpan newWithRandomSpanId(String i, MessagingModel m) {
            return new ChannelWithSpan(i, m, ThreadLocalRandom.current().nextLong());
        }

        public long getSpanId() {
            return sid;
        }
    }

    private static class StackStateImpl implements StackState<String> {
        private final int h; // depth.
        private final String s; // state.

        // Jackson JSON-lib needs a no-args constructor, but it can re-set finals.
        private StackStateImpl() {
            h = 0;
            s = null;
        }

        public StackStateImpl(int height, String state) {
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

    /**
     * MatsTraceStringImpl.toString().
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        CallImpl currentCall = getCurrentCall();

        if (currentCall == null) {
            return "MatsTrace w/o CurrentCall. TraceId:" + tid + ", FlowId:" + id + ".";
        }

        String callType = currentCall.getCallType().toString();
        callType = callType + spaces(8 - callType.length());

        // === HEADER ===
        buf.append("MatsTrace : ")
                .append(callType)
                .append(" #from:  ").append(currentCall.getFrom())
                .append("  KeepMatsTrace:").append(getKeepTrace())
                .append("  NonPersistent:").append(isNonPersistent())
                .append("  Interactive:").append(isInteractive())
                .append("  TTL:").append(((tl == null) || (tl == 0)) ? "forever" : tl.toString())
                .append("  NoAudit:").append(isNoAudit())
                .append('\n');

        buf.append("                     #to: ").append(currentCall.getTo())
                .append("  CallNumber:").append(getCallNumber())
                .append("  CurrentSpanId:").append(Long.toString(getCurrentSpanId(), 36))
                .append(currentCall.getCallType() == CallType.REPLY
                        ? "  ReplyFromSpanId:" + Long.toString(currentCall.getReplyFromSpanId(), 36)
                        : "")
                .append("  TraceId:'").append(getTraceId())
                .append("'\n");

        // === CURRENT CALL ===

        buf.append(" current call:\n")
                .append("    state:    ").append(getCurrentState()).append('\n')
                .append("    incoming: ").append(currentCall.getData()).append('\n');
        buf.append('\n');

        // === CALLS ===

        // --- Initiator "Call" ---
        buf.append(" call#:       call type\n");
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
        int maxStackSize = c.stream().mapToInt(CallImpl::getStackHeight).max().orElse(0);
        int maxToStageIdLength = c.stream()
                .mapToInt(c -> c.getTo().toString().length())
                .max().orElse(0);

        // --- Actual Calls (will be just the current if "MINIMAL") ---

        List<Call<String>> callFlow = getCallFlow();
        for (int i = 0; i < callFlow.size(); i++) {
            boolean printNullData = (kt == KeepMatsTrace.FULL) || (i == (callFlow.size() - 1));
            CallImpl call = (CallImpl) callFlow.get(i);
            buf.append(String.format("   %2d %s\n", i + 1,
                    call.toStringFromMatsTrace(ts, maxStackSize, maxToStageIdLength, printNullData)));
        }

        buf.append('\n');

        // === STATES ===

        // ?: Are we in FULL, meaning that there actually is a state flow?
        if (getKeepTrace() == KeepMatsTrace.FULL) {
            // -> Yes, FULL, so print the state flow
            buf.append(" ").append("state flow:\n");
            List<StackState<String>> stateFlow = getStateFlow();
            for (int i = 0; i < stateFlow.size(); i++) {
                buf.append(String.format("   %2d %s", i, stateFlow.get(i))).append('\n');
            }
            buf.append('\n');
        }

        // === REPLY TO STACK ===

        buf.append(" current ReplyTo stack: \n");
        List<Channel> stack = currentCall.getStack();
        if (stack.isEmpty()) {
            buf.append("    <empty, cannot reply>\n");
        }
        else {
            List<StackState<String>> stateStack = getStateStack();
            for (int i = 0; i < stack.size(); i++) {
                buf.append(String.format("   %2d %s", i, stack.get(i).toString()))
                        .append("  #state:").append(stateStack.get(i).getState())
                        .append('\n');
            }
        }
        buf.append('\n');

        // === SPAN ID STACK ===

        buf.append(" current SpanId stack: \n");
        List<Long> spanIdStack = getSpanIdStack();
        for (int i = 0; i < spanIdStack.size(); i++) {
            buf.append(String.format("   %2d %s", i,
                    Long.toString(spanIdStack.get(i), 36)));
            if (i == spanIdStack.size() - 1) {
                buf.append(" (SpanId which current " + currentCall.getCallType() + " call is processing within)");
            }
            if (i == 0) {
                buf.append(" (Root SpanId for initiator/terminator level)");
            }
            buf.append('\n');
        }

        return buf.toString();
    }
}
