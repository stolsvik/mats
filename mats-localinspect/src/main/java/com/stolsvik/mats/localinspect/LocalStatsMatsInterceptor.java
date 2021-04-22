package com.stolsvik.mats.localinspect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.api.intercept.MatsInitiateInterceptor.MatsInitiateInterceptOutgoingMessages;
import com.stolsvik.mats.api.intercept.MatsInterceptable;
import com.stolsvik.mats.api.intercept.MatsOutgoingMessage.MatsEditableOutgoingMessage;
import com.stolsvik.mats.api.intercept.MatsOutgoingMessage.MatsSentOutgoingMessage;
import com.stolsvik.mats.api.intercept.MatsOutgoingMessage.MessageType;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.MatsStageInterceptOutgoingMessages;
import com.stolsvik.mats.api.intercept.MatsStageInterceptor.StageCompletedContext.ProcessResult;

/**
 * Interceptor that collects "local stats" for Initiators and Stages of Endpoints, which can be used in conjunction with
 * a MatsFactory report generator, {@link LocalHtmlInspectForMatsFactory}.
 * <p />
 * To install, invoke the {@link #install(MatsInterceptable)} method, supplying the MatsFactory. The report generator
 * will fetch the interceptor from the MatsFactory.
 * <p />
 * <b>Implementation note:</b> Mats allows Initiators and Endpoints to be defined "runtime" - there is no specific setup
 * time vs. run time. This implies that the set of Endpoints and Initiators in a MatsFactory can increase from one run
 * of a report generation to the next (Endpoint may even be deleted, but this is really only meant for testing).
 * Moreover, this stats collecting class only collects stats for Initiators and Endpoints that have had traffic, since
 * it relies on the interceptors API. This means that while the MatsFactory might know about an Initiator or an
 * Endpoint, this stats class might not yet have picked it up. This is why all methods return Optional. On the other
 * hand, the set of Stages of an endpoint cannot change after it has been {@link MatsEndpoint#finishSetup()
 * finishedSetup()} (and it cannot be {@link MatsEndpoint#start() start()}'ed before it has been
 * <code>finishedSetup()</code>) - thus if e.g. only the initial stage of an Endpoint has so far seen traffic, this
 * class has nevertheless created stats objects for all of the Endpoint's stages.
 *
 * @author Endre Stølsvik 2021-04-09 00:37 - http://stolsvik.com/, endre@stolsvik.com
 */
public class LocalStatsMatsInterceptor
        implements MatsStageInterceptOutgoingMessages, MatsInitiateInterceptOutgoingMessages {

    public static final int DEFAULT_NUM_SAMPLES = 1100;

    /**
     * Creates an instance of this interceptor and installs it on the provided {@link MatsInterceptable} (which most
     * probably is a <code>MatsFactory</code>. Note that this interceptor is stateful wrt. the MatsFactory, thus a new
     * instance is needed per MatsFactory - which is fulfilled using this method. It should only be invoked once per
     * MatsFactory. You get the created interceptor in return, but that is not needed when employed with
     * {@link LocalHtmlInspectForMatsFactory}, as that will fetch the instance from the MatsFactory using
     * {@link MatsInterceptable#getInitiationInterceptorSingleton(Class)}.
     *
     * @param matsInterceptableMatsFactory
     *            the {@link MatsInterceptable} MatsFactory to add it to.
     */
    public static LocalStatsMatsInterceptor install(MatsInterceptable matsInterceptableMatsFactory) {
        LocalStatsMatsInterceptor interceptor = new LocalStatsMatsInterceptor(DEFAULT_NUM_SAMPLES);
        matsInterceptableMatsFactory.addInitiationInterceptorSingleton(interceptor);
        matsInterceptableMatsFactory.addStageInterceptorSingleton(interceptor);
        return interceptor;
    }

    private final int _numSamples;

    private LocalStatsMatsInterceptor(int numSamples) {
        _numSamples = numSamples;
    }

    private static final ConcurrentHashMap<MatsInitiator, InitiatorStatsImpl> _initiators = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<MatsEndpoint<?, ?>, EndpointStatsImpl> _endpoints = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<MatsStage<?, ?, ?>, StageStatsImpl> _stages = new ConcurrentHashMap<>();

    public static final String EXTRA_STATE_INITIATOR_NAME = "mats.ls.in";
    // public static final String EXTRA_STATE_REQUEST_TO = "mats.ls.rto";
    public static final String EXTRA_STATE_REQUEST_NANOS = "mats.ls.rts";
    public static final String EXTRA_STATE_REQUEST_NODENAME = "mats.ls.rnn";
    public static final String EXTRA_STATE_ENDPOINT_ENTER_NANOS = "mats.ls.eets";
    public static final String EXTRA_STATE_ENDPOINT_ENTER_NODENAME = "mats.ls.eenn";

    // ======================================================================================================
    // ===== Exposed API for LocalStatsMatsInterceptor

    public Optional<InitiatorStats> getInitiatorStats(MatsInitiator matsInitiator) {
        return Optional.ofNullable(_initiators.get(matsInitiator));
    }

    public Optional<EndpointStats> getEndpointStats(MatsEndpoint<?, ?> matsEndpoint) {
        return Optional.ofNullable(_endpoints.get(matsEndpoint));
    }

    public Optional<StageStats> getStageStats(MatsStage<?, ?, ?> matsStage) {
        return Optional.ofNullable(_stages.get(matsStage));
    }

    public interface InitiatorStats {
        StatsSnapshot getTotalExecutionTimeNanos();

        SortedMap<String, SortedMap<OutgoingMessageRepresentation, Long>> getOutgoingMessageCounts();
    }

    public interface EndpointStats {
        List<StageStats> getStagesStats();

        StageStats getStageStats(MatsStage<?, ?, ?> stage);

        StatsSnapshot getTotalEndpointProcessingNanos();
    }

    public interface StageStats {
        int getIndex();

        boolean isInitial();

        StatsSnapshot getTotalExecutionTimeNanos();

        StatsSnapshot getBetweenStagesTimeNanos();

        SortedMap<IncomingMessageRepresentation, Long> getIncomingMessageCounts();

        SortedMap<ProcessResult, Long> getProcessResultCounts();

        SortedMap<OutgoingMessageRepresentation, Long> getOutgoingMessageCounts();
    }

    interface StatsSnapshot {
        /**
         * @return the current set of samples. While starting, when we still haven't had max-samples yet, the array size
         *         will be lower than max-samples, down to 0 elements.
         */
        long[] getSamples();

        /**
         * @param percentile
         *            the requested percentile, in the range [0, 1]. 0 means the lowest value (i.e. all samples are >=
         *            this value), while 1 means the highest sample (i.e. all samples are <= this value).
         * @return the value at the desired percentile
         */
        long getValueAtPercentile(double percentile);

        /**
         * @return the number of executions so far, which can be > max-samples.
         */
        long getNumObservations();

        default long getMin() {
            return getValueAtPercentile(0);
        };

        double getAverage();

        default long getMax() {
            return getValueAtPercentile(1);
        };

        double getStdDev();

        default double getMedian() {
            return getValueAtPercentile(0.5);
        }

        default double get75thPercentile() {
            return getValueAtPercentile(0.75);
        }

        default double get95thPercentile() {
            return getValueAtPercentile(0.95);
        }

        default double get98thPercentile() {
            return getValueAtPercentile(0.98);
        }

        default double get99thPercentile() {
            return getValueAtPercentile(0.99);
        }

        default double get995thPercentile() {
            return getValueAtPercentile(0.995);
        }
    }

    public interface IncomingMessageRepresentation extends Comparable<IncomingMessageRepresentation> {
        MessageType getMessageType();

        String getFromAppName();

        String getFromStageId();

        String getInitiatingAppName();

        String getInitiatorId();
    }

    public interface OutgoingMessageRepresentation extends Comparable<OutgoingMessageRepresentation> {
        MessageType getMessageType();

        String getTo();
    }

    // ======================================================================================================
    // ===== INITIATION interceptor implementation

    @Override
    public void initiateInterceptOutgoingMessages(InitiateInterceptOutgoingMessagesContext context) {
        List<MatsEditableOutgoingMessage> outgoingMessages = context.getOutgoingMessages();

        // Decorate outgoing REQUESTs with extra-state (for the subsequent REPLY)
        outgoingMessages.stream()
                .filter(msg -> msg.getMessageType() == MessageType.REQUEST)
                .forEach(request -> {
                    // NOTE: *Initiation time* is already present in the MatsTrace and Mats API:
                    // DetachedProcessContext.getInitiatingTimestamp()
                    // The initiator name is not present in MatsTrace (not for the REPLY-receiver), adding it
                    request.setExtraStateForReplyOrNext(EXTRA_STATE_INITIATOR_NAME, context.getInitiator().getName());
                    // The initiation request to is not present in the MatsTrace (not for the REPLY-receiver), adding
                    // it.
                    // request.setExtraStateForReply(EXTRA_STATE_REQUEST_TO, request.getTo());
                });

        // TODO: Decorate outgoing SEND/PUBLISH too? Would use addString(..) for this.
    }

    @Override
    public void initiateCompleted(InitiateCompletedContext context) {
        MatsInitiator initiator = context.getInitiator();
        InitiatorStatsImpl initiatorStats = _initiators.computeIfAbsent(initiator, (v) -> new InitiatorStatsImpl(
                _numSamples));

        // :: TIMING
        long totalExecutionNanos = context.getTotalExecutionNanos();
        initiatorStats.addTimingSample(totalExecutionNanos);

        // :: OUTGOING MESSAGES
        List<MatsSentOutgoingMessage> outgoingMessages = context.getOutgoingMessages();
        for (MatsSentOutgoingMessage msg : outgoingMessages) {
            String initiatorId = msg.getInitiatorId();
            MessageType messageType = msg.getMessageType();
            String to = msg.getTo();
            initiatorStats.recordOutgoingMessage(initiatorId, messageType, to);
        }
    }

    // ======================================================================================================
    // ===== STAGE interceptor implementation

    @Override
    public void stageReceived(StageReceivedContext i_context) {
        MatsStage<?, ?, ?> stage = i_context.getStage();
        // Must get-or-create this first, since this is what creates the StageStats.
        getOrCreateEndpointStatsImpl(stage.getParentEndpoint());
        // Get the StageStats
        StageStatsImpl stageStats = _stages.get(stage);

        // Get the ProcesContext
        ProcessContext<Object> p_context = i_context.getProcessContext();

        // :: INCOMING MESSAGES:
        MessageType incomingMessageType = i_context.getIncomingMessageType();
        String fromAppName = p_context.getFromAppName();
        String fromStageId = p_context.getFromStageId();
        String initiatingAppName = p_context.getInitiatingAppName();
        String initiatorId = p_context.getInitiatorId();
        stageStats.recordIncomingMessage(incomingMessageType, fromAppName, fromStageId, initiatingAppName, initiatorId);

        // :: BETWEEN STAGES TIMING:
        // ?: Is this a reply?
        if ((incomingMessageType == MessageType.REPLY) || (incomingMessageType == MessageType.NEXT)) {
            // -> Yes, so then we should have extra-state for the time the request happened
            String requestNodename = i_context.getIncomingExtraState(EXTRA_STATE_REQUEST_NODENAME, String.class)
                    .orElse(null);
            if (stage.getParentEndpoint().getParentFactory().getFactoryConfig().getNodename().equals(requestNodename)) {
                long requestNanoTime = i_context.getIncomingExtraState(EXTRA_STATE_REQUEST_NANOS, Long.class)
                        .orElse(0L);
                stageStats.addBetweenStagesTimeMillis(System.nanoTime() - requestNanoTime);
            }
        }
    }

    @Override
    public void stageInterceptOutgoingMessages(StageInterceptOutgoingMessageContext context) {
        MatsStage<?, ?, ?> stage = context.getStage();
        // Must get-or-create this first, since this is what creates the StageStats.
        getOrCreateEndpointStatsImpl(stage.getParentEndpoint());

        // Get the StageStats
        StageStatsImpl stageStats = _stages.get(stage);

        // :: Add extra-state on outgoing messages for BETWEEN STAGES + ENDPOINT TOTAL PROCESSING TIME
        List<MatsEditableOutgoingMessage> outgoingMessages = context.getOutgoingMessages();
        outgoingMessages.stream()
                .filter(msg -> (msg.getMessageType() == MessageType.REQUEST)
                        || (msg.getMessageType() == MessageType.NEXT))
                .forEach(request -> {
                    // :: BETWEEN STAGES TIMING:
                    // The request timestamp is not present in the MatsTrace (not for the REPLY-receiver), adding it.
                    request.setExtraStateForReplyOrNext(EXTRA_STATE_REQUEST_NANOS, System.nanoTime());
                    request.setExtraStateForReplyOrNext(EXTRA_STATE_REQUEST_NODENAME, stage.getParentEndpoint()
                            .getParentFactory().getFactoryConfig().getNodename());

                    // The request /to/ is not present in the MatsTrace (not for the REPLY-receiver), adding it.
                    // request.setExtraStateForReply(EXTRA_STATE_REQUEST_TO, request.getTo());

                    // :: ENDPOINT TOTAL PROCESSING TIME
                    // NOTICE: Storing nanos - but also the node name, so that we'll only use the timing if it is
                    // same node exiting (or stopping, in case of terminator).
                    if (stageStats.isInitial()) {
                        request.setExtraStateForReplyOrNext(EXTRA_STATE_ENDPOINT_ENTER_NANOS, context.getStartedNanoTime());
                        request.setExtraStateForReplyOrNext(EXTRA_STATE_ENDPOINT_ENTER_NODENAME, stage.getParentEndpoint()
                                .getParentFactory().getFactoryConfig().getNodename());
                    }
                });
    }

    @Override
    public void stageCompleted(StageCompletedContext context) {
        MatsStage<?, ?, ?> stage = context.getStage();
        // Must get-or-create this first, since this is what creates the StageStats.
        EndpointStatsImpl endpointStats = getOrCreateEndpointStatsImpl(stage.getParentEndpoint());

        // Get the StageStats
        StageStatsImpl stageStats = _stages.get(stage);

        // :: TOTAL EXECUTIION TIMING
        stageStats.addTotalExecutionTimeNanos(context.getTotalExecutionNanos());

        // :: PROCESS RESULTS
        ProcessResult processResult = context.getProcessResult();
        stageStats.recordProcessResult(processResult);

        // :: ENDPOINT TOTAL PROCESSING TIME
        // ?: Is this an "exiting process result", i.e. either REPLY (service) or NONE (terminator)?
        if (processResult == ProcessResult.REPLY || (processResult == ProcessResult.NONE)) {
            // -> Yes, "exiting process result" - record endpoint total processing time
            // ?: Is this the initial stage?
            if (stageStats.isInitial()) {
                // -> Yes, initial - thus this is the only stage processed
                endpointStats.addEndpointTotalProcessingTime(System.nanoTime() - context.getStartedNanoTime());
            }
            else {
                // -> No, not initial stage, been through one or more request/reply calls
                String enterNodename = context.getIncomingExtraState(EXTRA_STATE_ENDPOINT_ENTER_NODENAME, String.class)
                        .orElse(null);
                // ?: Was the initial message on the same node as this processing?
                if (stage.getParentEndpoint().getParentFactory().getFactoryConfig().getNodename().equals(
                        enterNodename)) {
                    // -> Yes, same node - so then the System.nanoTime() is relevant to compare
                    Long enterNanoTime = context.getIncomingExtraState(EXTRA_STATE_ENDPOINT_ENTER_NANOS, Long.class)
                            .orElse(0L);
                    endpointStats.addEndpointTotalProcessingTime(System.nanoTime() - enterNanoTime);
                }
            }
        }

        // - FOR TERMINATORS: time since mats flow was initiated:
        long timeSinceInit = System.currentTimeMillis() - context.getProcessContext().getInitiatingTimestamp()
                .toEpochMilli();

        // :: OUTGOING MESSAGES
        List<MatsSentOutgoingMessage> outgoingMessages = context.getOutgoingMessages();
        for (MatsSentOutgoingMessage msg : outgoingMessages) {
            MessageType messageType = msg.getMessageType();
            String to = msg.getTo();
            stageStats.recordOutgoingMessage(messageType, to);
        }
    }

    private EndpointStatsImpl getOrCreateEndpointStatsImpl(MatsEndpoint<?, ?> endpoint) {
        return _endpoints.computeIfAbsent(endpoint, (v) -> {
            EndpointStatsImpl epStats = new EndpointStatsImpl(endpoint, _numSamples);
            // Side-effect: Put all the stages into the stages-map, for direct access.
            _stages.putAll(epStats.getStagesMap());
            return epStats;
        });
    }

    // ======================================================================================================
    // ===== ..internals..

    static class InitiatorStatsImpl implements InitiatorStats {
        private final RingBuffer_Long _totalExecutionTimesNanos;

        private final ConcurrentHashMap<OutgoingMessageRepresentationImpl, AtomicLong> _outgoingMessageCounts = new ConcurrentHashMap<>();

        public InitiatorStatsImpl(int numSamples) {
            _totalExecutionTimesNanos = new RingBuffer_Long(numSamples);
        }

        public void addTimingSample(long totalExecutionTime) {
            _totalExecutionTimesNanos.addEntry(totalExecutionTime);
        }

        private void recordOutgoingMessage(String initiatorId, MessageType messageType, String to) {
            OutgoingMessageRepresentationImpl msg =
                    new OutgoingMessageRepresentationImpl(initiatorId, messageType, to);
            AtomicLong count = _outgoingMessageCounts.computeIfAbsent(msg, x -> new AtomicLong());
            count.incrementAndGet();
        }

        @Override
        public StatsSnapshot getTotalExecutionTimeNanos() {
            return new StatsSnapshotImpl(_totalExecutionTimesNanos.getValuesCopy(),
                    _totalExecutionTimesNanos.getNumExecutions());
        }

        @Override
        public SortedMap<String, SortedMap<OutgoingMessageRepresentation, Long>> getOutgoingMessageCounts() {
            SortedMap<String, SortedMap<OutgoingMessageRepresentation, Long>> ret = new TreeMap<>();
            _outgoingMessageCounts.forEach((msg, v) -> {
                Map<OutgoingMessageRepresentation, Long> inner =
                        ret.computeIfAbsent(msg.getInitiatorId(), s -> new TreeMap<>());
                inner.put(msg, v.get());
            });
            return ret;
        }
    }

    public static class EndpointStatsImpl implements EndpointStats {
        private final Map<MatsStage<?, ?, ?>, StageStatsImpl> _stagesMap;
        private final List<StageStats> _stageStats;
        private final List<StageStats> _stageStats_unmodifiable;

        private final RingBuffer_Long _totalEndpointProcessingNanos;

        private EndpointStatsImpl(MatsEndpoint<?, ?> endpoint, int sampleReservoirSize) {
            List<? extends MatsStage<?, ?, ?>> stages = endpoint.getStages();
            _stagesMap = new HashMap<>(stages.size());
            _stageStats = new ArrayList<>(stages.size());
            // :: Create StateStatsImpl for each Stage of the Endpoint.
            // Note: No use in adding "between stages time millis" sample reservoir for the first stage..
            for (int i = 0; i < stages.size(); i++) {
                MatsStage<?, ?, ?> stage = stages.get(i);
                StageStatsImpl stageStats = new StageStatsImpl(sampleReservoirSize, i);
                _stagesMap.put(stage, stageStats);
                _stageStats.add(stageStats);
            }
            _stageStats_unmodifiable = Collections.unmodifiableList(_stageStats);

            _totalEndpointProcessingNanos = new RingBuffer_Long(sampleReservoirSize);
        }

        private StageStatsImpl getStageStatsImpl(MatsStage<?, ?, ?> stage) {
            return _stagesMap.get(stage);
        }

        public Map<MatsStage<?, ?, ?>, StageStatsImpl> getStagesMap() {
            return _stagesMap;
        }

        private void addEndpointTotalProcessingTime(long nanosEndpointTotalProcessingTime) {
            _totalEndpointProcessingNanos.addEntry(nanosEndpointTotalProcessingTime);
        }

        @Override
        public List<StageStats> getStagesStats() {
            return _stageStats_unmodifiable;
        }

        @Override
        public StageStats getStageStats(MatsStage<?, ?, ?> stage) {
            return _stagesMap.get(stage);
        }

        @Override
        public StatsSnapshot getTotalEndpointProcessingNanos() {
            return new StatsSnapshotImpl(_totalEndpointProcessingNanos.getValuesCopy(),
                    _totalEndpointProcessingNanos.getNumExecutions());
        }
    }

    static class StageStatsImpl implements StageStats {
        private final RingBuffer_Long _totalExecutionTimeNanos;
        private final RingBuffer_Long _betweenStagesTimeMillis;
        private final int _index;
        private final boolean _initial;

        private final ConcurrentHashMap<IncomingMessageRepresentationImpl, AtomicLong> _incomingMessageCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<OutgoingMessageRepresentationImpl, AtomicLong> _outgoingMessageCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<ProcessResult, AtomicLong> _processResultCounts = new ConcurrentHashMap<>();

        public StageStatsImpl(int sampleReservoirSize, int index) {
            _totalExecutionTimeNanos = new RingBuffer_Long(sampleReservoirSize);
            _index = index;
            _initial = index == 0;
            _betweenStagesTimeMillis = (index == 0)
                    ? null
                    : new RingBuffer_Long(sampleReservoirSize);
        }

        private void addTotalExecutionTimeNanos(long totalExecutionTimeNanos) {
            _totalExecutionTimeNanos.addEntry(totalExecutionTimeNanos);
        }

        private void addBetweenStagesTimeMillis(long betweenStagesTimeMillis) {
            _betweenStagesTimeMillis.addEntry(betweenStagesTimeMillis);
        }

        private void recordIncomingMessage(MessageType incomingMessageType, String fromAppName, String fromStageId,
                String initiatingAppName, String initiatorId) {
            IncomingMessageRepresentationImpl incomingMessageRepresentation = new IncomingMessageRepresentationImpl(
                    incomingMessageType, fromAppName, fromStageId, initiatingAppName, initiatorId);
            AtomicLong count = _incomingMessageCounts.computeIfAbsent(incomingMessageRepresentation,
                    x -> new AtomicLong());
            count.incrementAndGet();
        }

        private void recordProcessResult(ProcessResult processResult) {
            AtomicLong count = _processResultCounts.computeIfAbsent(processResult, x -> new AtomicLong());
            count.incrementAndGet();
        }

        private void recordOutgoingMessage(MessageType messageType, String to) {
            OutgoingMessageRepresentationImpl msg = new OutgoingMessageRepresentationImpl(messageType, to);
            AtomicLong count = _outgoingMessageCounts.computeIfAbsent(msg, x -> new AtomicLong());
            count.incrementAndGet();
        }

        @Override
        public int getIndex() {
            return _index;
        }

        @Override
        public boolean isInitial() {
            return _initial;
        }

        @Override
        public StatsSnapshot getTotalExecutionTimeNanos() {
            return new StatsSnapshotImpl(_totalExecutionTimeNanos.getValuesCopy(),
                    _totalExecutionTimeNanos.getNumExecutions());
        }

        @Override
        public StatsSnapshot getBetweenStagesTimeNanos() {
            if (_betweenStagesTimeMillis == null) {
                return null;
            }
            return new StatsSnapshotImpl(_betweenStagesTimeMillis.getValuesCopy(),
                    _betweenStagesTimeMillis.getNumExecutions());
        }

        @Override
        public SortedMap<IncomingMessageRepresentation, Long> getIncomingMessageCounts() {
            SortedMap<IncomingMessageRepresentation, Long> ret = new TreeMap<>();
            _incomingMessageCounts.forEach((k, v) -> ret.put(k, v.get()));
            return ret;
        }

        @Override
        public SortedMap<ProcessResult, Long> getProcessResultCounts() {
            SortedMap<ProcessResult, Long> ret = new TreeMap<>();
            _processResultCounts.forEach((k, v) -> ret.put(k, v.get()));
            return ret;
        }

        @Override
        public SortedMap<OutgoingMessageRepresentation, Long> getOutgoingMessageCounts() {
            SortedMap<OutgoingMessageRepresentation, Long> ret = new TreeMap<>();
            _outgoingMessageCounts.forEach((k, v) -> ret.put(k, v.get()));
            return ret;
        }
    }

    // ======================================================================================================
    // ===== Impl: OutgoingMessageRepresentation & IncomingMessageRepresentation


    public static class OutgoingMessageRepresentationImpl implements OutgoingMessageRepresentation {
        private final MessageType _messageType;
        private final String _to;

        private final String _initiatorId;

        public OutgoingMessageRepresentationImpl(MessageType messageType, String to) {
            _messageType = messageType;
            _to = to;
            _initiatorId = null;
        }

        public OutgoingMessageRepresentationImpl(String initiatorId, MessageType messageType, String to) {
            _messageType = messageType;
            _to = to;
            _initiatorId = initiatorId;
        }

        private String getInitiatorId() {
            return _initiatorId;
        }

        @Override
        public MessageType getMessageType() {
            return _messageType;
        }

        @Override
        public String getTo() {
            return _to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OutgoingMessageRepresentationImpl that = (OutgoingMessageRepresentationImpl) o;
            return _messageType == that._messageType
                    && Objects.equals(_initiatorId, that._initiatorId)
                    && Objects.equals(_to, that._to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(_messageType, _to, _initiatorId);
        }

        private static final Comparator<OutgoingMessageRepresentation> COMPARATOR = Comparator
                .comparing(OutgoingMessageRepresentation::getMessageType)
                .thenComparing(OutgoingMessageRepresentation::getTo);

        @Override
        public int compareTo(OutgoingMessageRepresentation o) {
            return COMPARATOR.compare(this, o);
        }
    }

    public static class IncomingMessageRepresentationImpl implements IncomingMessageRepresentation {
        private final MessageType _messageType;
        private final String _fromAppName;
        private final String _fromStageId;
        private final String _initiatingAppName;
        private final String _initiatorId;

        public IncomingMessageRepresentationImpl(MessageType messageType, String fromAppName, String fromStageId,
                String initiatingAppName, String initiatorId) {
            _messageType = messageType;
            _fromAppName = fromAppName;
            _fromStageId = fromStageId;
            _initiatingAppName = initiatingAppName;
            _initiatorId = initiatorId;
        }

        public MessageType getMessageType() {
            return _messageType;
        }

        public String getFromAppName() {
            return _fromAppName;
        }

        public String getFromStageId() {
            return _fromStageId;
        }

        public String getInitiatingAppName() {
            return _initiatingAppName;
        }

        public String getInitiatorId() {
            return _initiatorId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IncomingMessageRepresentationImpl that = (IncomingMessageRepresentationImpl) o;
            return _messageType == that._messageType
                    && Objects.equals(_fromAppName, that._fromAppName)
                    && Objects.equals(_fromStageId, that._fromStageId)
                    && Objects.equals(_initiatingAppName, that._initiatingAppName)
                    && Objects.equals(_initiatorId, that._initiatorId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(_messageType, _fromAppName, _fromStageId, _initiatingAppName, _initiatorId);
        }

        private static final Comparator<IncomingMessageRepresentation> COMPARATOR = Comparator
                .comparing(IncomingMessageRepresentation::getMessageType)
                .thenComparing(IncomingMessageRepresentation::getFromStageId)
                .thenComparing(IncomingMessageRepresentation::getFromAppName)
                .thenComparing(IncomingMessageRepresentation::getInitiatorId)
                .thenComparing(IncomingMessageRepresentation::getInitiatingAppName);

        @Override
        public int compareTo(IncomingMessageRepresentation o) {
            return COMPARATOR.compare(this, o);
        }
    }



    // ===== Stats and RingBuffer

    static class StatsSnapshotImpl implements StatsSnapshot {
        private final long[] _values;
        private final long _numExecutions;

        /**
         * NOTE!! The values array shall be "owned" by this instance (i.e. copied out), and WILL be sorted here.
         */
        public StatsSnapshotImpl(long[] values, long numExecutions) {
            _values = values;
            Arrays.sort(_values);
            _numExecutions = numExecutions;
        }

        @Override
        public long[] getSamples() {
            return _values;
        }

        @Override
        public long getValueAtPercentile(double percentile) {
            if (_values.length == 0) {
                return 0;
            }
            if (percentile == 0) {
                return _values[0];
            }
            if (percentile == 1) {
                return _values[_values.length - 1];
            }
            double index = percentile * (_values.length - 1);
            double rint = Math.rint(index);
            // ?: Is it a whole number?
            if (index == Math.rint(index)) {
                // -> Yes, whole number
                return _values[(int) index];
            }
            // E-> No, not whole number
            double firstIndex = Math.floor(index);
            double remainder = index - firstIndex;
            long first = _values[(int) firstIndex];
            long second = _values[((int) firstIndex) + 1];
            return (long) (first * (1 - remainder) + second * remainder);
        }

        @Override
        public long getNumObservations() {
            return _numExecutions;
        }

        @Override
        public double getAverage() {
            // ?: Do we have no samples?
            if (_values.length == 0) {
                // -> Yes, no samples - return 0
                return 0;
            }
            long sum = 0;
            for (long value : _values) {
                sum += value;
            }
            return sum / (double) _values.length;
        }

        @Override
        public double getStdDev() {
            // ?: Do we have only 0 or 1 samples? ("sample mean" uses 'n - 1' as divisor)
            if (_values.length <= 1) {
                // -> Yes, no samples, or only 1 - return 0
                return 0;
            }
            double avg = getAverage();
            double sd = 0;
            for (long value : _values) {
                sd += Math.pow(((double) value) - avg, 2);
            }
            // Use /sample/ standard deviation (not population), i.e. "n - 1" as divisor.
            return Math.sqrt(sd / (double) (_values.length - 1));
        }
    }

    private static class RingBuffer_Long {
        private final long[] _values;
        private final int _sampleReservoirSize;
        private final LongAdder _numAdded;

        private int _bufferPos;

        private RingBuffer_Long(int sampleReservoirSize) {
            _values = new long[sampleReservoirSize];
            _sampleReservoirSize = sampleReservoirSize;
            _numAdded = new LongAdder();
            _bufferPos = 0;
        }

        void addEntry(long entry) {
            synchronized (this) {
                // First add at current pos
                _values[_bufferPos] = entry;
                // Now increase and wrap current pos
                _bufferPos = ++_bufferPos % _sampleReservoirSize;
            }
            // Keep tally of total number added
            _numAdded.increment();
        }

        long[] getValuesCopy() {
            // Note: There is a race here, but the error is always at most excluding one or a few samples - no problem.
            int numAdded = _numAdded.intValue();
            synchronized (this) {
                // ?: Already filled the ring?
                if (numAdded >= _sampleReservoirSize) {
                    // -> Yes, ring is filled.
                    // Use clone
                    return _values.clone();
                }
                // E-> no, ring is not filled, so we need to do partial copy
                long[] copy = new long[numAdded];
                System.arraycopy(_values, 0, copy, 0, numAdded);
                return copy;
            }
        }

        /**
         * @return the buffer size, i.e. max samples that will be kept, when older are discarded.
         */
        public int getSampleReservoirSize() {
            return _sampleReservoirSize;
        }

        /**
         * @return the number of samples in the buffer, which is capped by {@link #getSampleReservoirSize()}.
         */
        public int getSamplesInReservoir() {
            return Math.min(_numAdded.intValue(), _sampleReservoirSize);
        }

        /**
         * @return the total number of executions run - of which only the last {@link #getSampleReservoirSize()} is kept
         *         in the circular buffer.
         */
        public long getNumExecutions() {
            return _numAdded.longValue();
        }
    }

    public static void main(String[] args) {
        RingBuffer_Long list = new RingBuffer_Long(11);
        long total = 0;
        int added = 0;
        for (int i = 0; i < 40; i++) {
            list.addEntry(i);
            total += i;
            added++;

            // System.out.println("[" + i + "] local: " + (total / (double) added) + ", list:" + list.getAverage());
        }
    }
}
