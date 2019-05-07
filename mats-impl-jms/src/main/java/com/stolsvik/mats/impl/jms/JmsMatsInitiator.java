package com.stolsvik.mats.impl.jms;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessLambda;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import com.stolsvik.mats.impl.jms.JmsMatsProcessContext.DoAfterCommitRunnableHolder;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.TransactionContext;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager_JmsOnly.JmsMatsMessageSendException;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsSerializer.DeserializedMatsTrace;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call;
import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;
import com.stolsvik.mats.serial.MatsTrace.KeepMatsTrace;

class JmsMatsInitiator<Z> implements MatsInitiator, JmsMatsTxContextKey, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiator.class);

    private final JmsMatsFactory<Z> _parentFactory;
    private final JmsMatsJmsSessionHandler _jmsMatsJmsSessionHandler;
    private final TransactionContext _transactionContext;

    public JmsMatsInitiator(JmsMatsFactory<Z> parentFactory, JmsMatsJmsSessionHandler jmsMatsJmsSessionHandler,
            JmsMatsTransactionManager jmsMatsTransactionManager) {
        // NOTICE! Due to multi-threading, whereby one Initiator might be used "globally" for e.g. a Servlet Container
        // having 200 threads, we cannot fetch a sole Session for the Initiator to be used for all initiations (as
        // it might be used concurrently by all the 200 Servlet Container threads). Thus, each initiation needs to
        // get hold of its own Session. However, the Sessions should be pooled.

        _parentFactory = parentFactory;
        _jmsMatsJmsSessionHandler = jmsMatsJmsSessionHandler;
        _transactionContext = jmsMatsTransactionManager.getTransactionContext(this);
    }

    @Override
    public void initiate(InitiateLambda lambda) throws MatsBackendException, MatsMessageSendException {
        // NOTICE! Due to multi-threading, whereby one Initiator might be used "globally" for e.g. a Servlet Container
        // having 200 threads, we cannot fetch a sole Session for the Initiator to be used for all initiations (as
        // it might be used concurrently by all the 200 Servlet Container threads). Thus, each initiation needs to
        // get hold of its own Session. However, the Sessions should be pooled.

        // TODO / OPTIMIZE: Consider doing lazy init for TransactionContext too
        // as well as being able to "open" again after close? What about introspection/monitoring/instrumenting -
        // that is, "turn off" a MatsInitiator: Either hang requsts, or probably more interesting, fail them. And
        // that would be nice to use "close()" for: As long as it is closed, it can't be used. Need to evaluate.

        long nanosStart = System.nanoTime();
        JmsSessionHolder jmsSessionHolder;
        try {
            jmsSessionHolder = _jmsMatsJmsSessionHandler.getSessionHolder(this);
        }
        catch (JmsMatsJmsException e) {
            // Could not get hold of JMS *Connection* - Read the JavaDoc of JmsMatsJmsSessionHandler.getSessionHolder()
            throw new MatsBackendException("Could not get hold of JMS Connection.", e);
        }
        try {
            DoAfterCommitRunnableHolder doAfterCommitRunnableHolder = new DoAfterCommitRunnableHolder();
            _transactionContext.doTransaction(jmsSessionHolder, () -> {
                List<JmsMatsMessage<Z>> messagesToSend = new ArrayList<>();
                lambda.initiate(new JmsMatsInitiate<>(_parentFactory, messagesToSend, doAfterCommitRunnableHolder));
                sendMatsMessages(log, nanosStart, jmsSessionHolder, _parentFactory, messagesToSend);
            });
            jmsSessionHolder.release();
            // :: Handle the context.doAfterCommit(Runnable) lambda.
            try {
                doAfterCommitRunnableHolder.runDoAfterCommitIfAny();
            }
            catch (RuntimeException re) {
                log.error(LOG_PREFIX
                        + "Got RuntimeException when running the doAfterCommit Runnable."
                        + " Ignoring.", re);
            }
        }
        catch (JmsMatsMessageSendException e) {
            // JmsMatsMessageSendException is a JmsMatsJmsException, and that indicates that there was a problem with
            // JMS - so we should "crash" the JmsSessionHolder to signal that the JMS Connection is probably broken.
            jmsSessionHolder.crashed(e);
            // This is a special variant of JmsMatsJmsException which is the "VERY BAD!" scenario.
            // TODO: Do retries if it fails!
            throw new MatsMessageSendException("Evidently got problems sending out the JMS message after having run the"
                    + " process lambda and potentially committed other resources, typically database.", e);
        }
        catch (JmsMatsJmsException e) {
            // Catch any JmsMatsJmsException, as that indicates that there was a problem with JMS - so we should
            // "crash" the JmsSessionHolder to signal that the JMS Connection is probably broken.
            // Notice that we shall NOT have committed "external resources" at this point, meaning database.
            jmsSessionHolder.crashed(e);
            // .. then throw on. This is a lesser evil than JmsMatsMessageSendException, as it probably have happened
            // before we committed database etc.
            throw new MatsBackendException("Evidently have problems talking with our backend, which is a JMS Broker.",
                    e);
        }
    }

    @Override
    public void initiateUnchecked(InitiateLambda lambda) throws MatsBackendRuntimeException,
            MatsMessageSendRuntimeException {
        try {
            initiate(lambda);
        }
        catch (MatsMessageSendException e) {
            throw new MatsMessageSendRuntimeException("Wrapping the MatsMessageSendException in a unchecked variant",
                    e);
        }
        catch (MatsBackendException e) {
            throw new MatsBackendRuntimeException("Wrapping the MatsBackendException in a unchecked variant", e);
        }
    }

    @Override
    public void close() {
        // TODO: Implement close semantics: Close all Sessions in any pool, and close Connection(s).
    }

    @Override
    public JmsMatsStage<?, ?, ?, ?> getStage() {
        // There is no stage, and contract is to return null.
        return null;
    }

    @Override
    public JmsMatsFactory<Z> getFactory() {
        return _parentFactory;
    }

    @Override
    public String toString() {
        return idThis();
    }

    static class JmsMatsInitiate<Z> implements MatsInitiate, JmsMatsStatics {
        private static final Logger log = LoggerFactory.getLogger(JmsMatsInitiate.class);

        private final JmsMatsFactory<Z> _parentFactory;
        private final List<JmsMatsMessage<Z>> _messagesToSend;
        private final DoAfterCommitRunnableHolder _doAfterCommitRunnableHolder;

        JmsMatsInitiate(JmsMatsFactory<Z> parentFactory, List<JmsMatsMessage<Z>> messagesToSend, DoAfterCommitRunnableHolder doAfterCommitRunnableHolder) {
            _parentFactory = parentFactory;
            _messagesToSend = messagesToSend;
            _doAfterCommitRunnableHolder = doAfterCommitRunnableHolder;
        }

        private MatsTrace<Z> _existingMatsTrace;

        JmsMatsInitiate(JmsMatsFactory<Z> parentFactory, List<JmsMatsMessage<Z>> messagesToSend, DoAfterCommitRunnableHolder doAfterCommitRunnableHolder,
                MatsTrace<Z> existingMatsTrace, Map<String, Object> propsSetInStage) {
            _parentFactory = parentFactory;
            _messagesToSend = messagesToSend;
            _doAfterCommitRunnableHolder = doAfterCommitRunnableHolder;

            _existingMatsTrace = existingMatsTrace;

            // Set the initial traceId, if the user chooses to not set it
            _traceId = existingMatsTrace.getTraceId();
            _from = existingMatsTrace.getCurrentCall().getFrom();

            // Copy over the properties which so far has been set in the stage (before this message is initiated).
            _props.putAll(propsSetInStage);
        }

        private String _traceId;
        private KeepMatsTrace _keepTrace;
        private boolean _nonPersistent;
        private boolean _interactive;
        private String _from;
        private String _to;
        private String _replyTo;
        private boolean _replyToSubscription;
        private Object _replySto;
        private final LinkedHashMap<String, Object> _props = new LinkedHashMap<>();
        private final LinkedHashMap<String, byte[]> _binaries = new LinkedHashMap<>();
        private final LinkedHashMap<String, String> _strings = new LinkedHashMap<>();

        @Override
        public MatsInitiate traceId(String traceId) {
            // ?: If we're an initiation from within a stage, append the traceId to the existing traceId, else set.
            _traceId = (_existingMatsTrace != null ? _existingMatsTrace.getTraceId() + '|' + traceId : traceId);
            return this;
        }

        @Override
        public MatsInitiate keepTrace(KeepTrace keepTrace) {
            if (keepTrace == KeepTrace.MINIMAL) {
                _keepTrace = KeepMatsTrace.MINIMAL;
            }
            else if (keepTrace == KeepTrace.COMPACT) {
                _keepTrace = KeepMatsTrace.COMPACT;
            }
            else if (keepTrace == KeepTrace.FULL) {
                _keepTrace = KeepMatsTrace.FULL;
            }
            else {
                throw new IllegalArgumentException("Unknown KeepTrace enum [" + keepTrace + "].");
            }
            return this;
        }

        @Override
        public MatsInitiate nonPersistent() {
            _nonPersistent = true;
            return this;
        }

        @Override
        public MatsInitiate interactive() {
            _interactive = true;
            return this;
        }

        @Override
        public MatsInitiate from(String initiatorId) {
            _from = initiatorId;
            return this;
        }

        @Override
        public MatsInitiate to(String endpointId) {
            _to = endpointId;
            return this;
        }

        @Override
        public MatsInitiate replyTo(String endpointId, Object replySto) {
            _replyTo = endpointId;
            _replySto = replySto;
            _replyToSubscription = false;
            return this;
        }

        @Override
        public MatsInitiate replyToSubscription(String endpointId, Object replySto) {
            _replyTo = endpointId;
            _replySto = replySto;
            _replyToSubscription = true;
            return this;
        }

        @Override
        public MatsInitiate setTraceProperty(String propertyName, Object propertyValue) {
            _props.put(propertyName, propertyValue);
            return this;
        }

        @Override
        public MatsInitiate addBytes(String key, byte[] payload) {
            _binaries.put(key, payload);
            return this;
        }

        @Override
        public MatsInitiate addString(String key, String payload) {
            _strings.put(key, payload);
            return this;
        }

        @Override
        public void request(Object requestDto) {
            request(requestDto, null);
        }

        @Override
        public void request(Object requestDto, Object initialTargetSto) {
            long nanosStart = System.nanoTime();
            String msg = "All of 'traceId', 'from', 'to' and 'replyTo' must be set when request(..)";
            checkCommon(msg);
            if (_replyTo == null) {
                throw new NullPointerException(msg + ": Missing 'replyTo'.");
            }
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            long now = System.currentTimeMillis();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId, _keepTrace, _nonPersistent, _interactive)
                    // TODO: Add debug info!
                    .setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                            _parentFactory.getFactoryConfig().getAppVersion(),
                            _parentFactory.getFactoryConfig().getNodename(), _from, now, "Tralala!")
                    .addRequestCall(_from,
                            _to, MessagingModel.QUEUE,
                            _replyTo, (_replyToSubscription ? MessagingModel.TOPIC : MessagingModel.QUEUE),
                            ser.serializeObject(requestDto),
                            ser.serializeObject(_replySto),
                            ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    _parentFactory.getFactoryConfig().getNodename(), now, "Callalala!");

            // Produce the new REQUEST JmsMatsMessage to send
            JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory.getMatsSerializer(),
                    matsTrace, _props, _binaries, _strings, "new REQUEST");
            _messagesToSend.add(request);
        }

        @Override
        public void send(Object messageDto) {
            send(messageDto, null);
        }

        @Override
        public void send(Object messageDto, Object initialTargetSto) {
            long nanosStart = System.nanoTime();
            checkCommon("All of 'traceId', 'from' and 'to' must be set when send(..)");
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            long now = System.currentTimeMillis();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId, _keepTrace, _nonPersistent, _interactive)
                    // TODO: Add debug info!
                    .setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                            _parentFactory.getFactoryConfig().getAppVersion(),
                            _parentFactory.getFactoryConfig().getNodename(), _from, now, "Tralala!")
                    .addSendCall(_from,
                            _to, MessagingModel.QUEUE,
                            ser.serializeObject(messageDto), ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    _parentFactory.getFactoryConfig().getNodename(), now, "Callalala!");

            // Produce the new SEND JmsMatsMessage to send
            JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory.getMatsSerializer(),
                    matsTrace, _props, _binaries, _strings, "new SEND");
            _messagesToSend.add(request);
        }

        @Override
        public void publish(Object messageDto) {
            publish(messageDto, null);
        }

        @Override
        public void publish(Object messageDto, Object initialTargetSto) {
            long nanosStart = System.nanoTime();
            checkCommon("All of 'traceId', 'from' and 'to' must be set when publish(..)");
            MatsSerializer<Z> ser = _parentFactory.getMatsSerializer();
            long now = System.currentTimeMillis();
            MatsTrace<Z> matsTrace = ser.createNewMatsTrace(_traceId, _keepTrace, _nonPersistent, _interactive)
                    // TODO: Add debug info!
                    .setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                            _parentFactory.getFactoryConfig().getAppVersion(),
                            _parentFactory.getFactoryConfig().getNodename(), _from, now, "Tralala!")
                    .addSendCall(_from,
                            _to, MessagingModel.TOPIC,
                            ser.serializeObject(messageDto), ser.serializeObject(initialTargetSto));

            copyOverAnyExistingTraceProperties(matsTrace);

            // TODO: Add debug info!
            matsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                    _parentFactory.getFactoryConfig().getAppVersion(),
                    _parentFactory.getFactoryConfig().getNodename(), now, "Callalala!");

            // Produce the new PUBLISH JmsMatsMessage to send
            JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory.getMatsSerializer(),
                    matsTrace, _props, _binaries, _strings, "new PUBLISH");
            _messagesToSend.add(request);
        }

        private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

        @Override
        public <R, S, I> void unstash(byte[] stash,
                Class<R> replyClass, Class<S> stateClass, Class<I> incomingClass,
                ProcessLambda<R, S, I> lambda) {

            long nanosStart = System.nanoTime();

            if (stash == null) {
                throw new NullPointerException("byte[] stash");
            }

            // :: Validate that this is a "MATSjmts" v.1 stash.
            validateByte(stash, 0, 77);
            validateByte(stash, 1, 65);
            validateByte(stash, 2, 84);
            validateByte(stash, 3, 83);
            validateByte(stash, 4, 106);
            validateByte(stash, 5, 109);
            validateByte(stash, 6, 116);
            validateByte(stash, 7, 115);
            validateByte(stash, 8, 1);

            // ----- Validated ok. Could have thrown in a checksum, but if foot-shooting is your thing, then go ahead.

            // ::: Get the annoying metadata

            // How many such fields are there. The idea is that we can add more fields in later revisions, and
            // just have older versions "jump over" the ones it does not know.
            int howManyZeros = stash[9];

            // :: Find zeros (field delimiters)
            int zstartEndpointId = findZero(stash, 10); // Should currently be right there, at pos#10.
            int zstartStageId = findZero(stash, zstartEndpointId + 1);
            int zstartNextStageId = findZero(stash, zstartStageId + 1);
            int zstartMatsTraceMeta = findZero(stash, zstartNextStageId + 1);
            int zstartMessageId = findZero(stash, zstartMatsTraceMeta + 1);
            // :: Here we'll jump over fields that we do not know, to be able to add more metadata in later revisions.
            int zstartMatsTrace = zstartMessageId;
            for (int i = 5; i < howManyZeros; i++) {
                zstartMatsTrace = findZero(stash, zstartMatsTrace + 1);
            }

            // :: Metadata
            // :EndpointId
            String endpointId = new String(stash, zstartEndpointId + 1, zstartStageId - zstartEndpointId - 1,
                    CHARSET_UTF8);
            // :StageId
            String stageId = new String(stash, zstartStageId + 1, zstartNextStageId - zstartStageId - 1, CHARSET_UTF8);
            // :NextStageId
            // If nextStageId == the special "no next stage" string, then null. Else get it.
            String nextStageId = (zstartMatsTraceMeta - zstartNextStageId
                    - 1) == JmsMatsProcessContext.NO_NEXT_STAGE.length &&
                    stash[zstartNextStageId + 1] == JmsMatsProcessContext.NO_NEXT_STAGE[0]
                            ? null
                            : new String(stash, zstartNextStageId + 1,
                                    zstartMatsTraceMeta - zstartNextStageId - 1, CHARSET_UTF8);
            // :MatsTrace Meta
            String matsTraceMeta = new String(stash, zstartMatsTraceMeta + 1,
                    zstartMessageId - zstartMatsTraceMeta - 1, CHARSET_UTF8);
            // :MessageId
            String messageId = new String(stash, zstartMessageId + 1,
                    zstartMatsTrace - zstartMessageId - 1, CHARSET_UTF8);

            // :Actual MatsTrace:
            DeserializedMatsTrace<Z> deserializedMatsTrace = _parentFactory.getMatsSerializer()
                    .deserializeMatsTrace(stash, zstartMatsTrace + 1,
                            stash.length - zstartMatsTrace - 1, matsTraceMeta);
            MatsTrace<Z> matsTrace = deserializedMatsTrace.getMatsTrace();

            // :: Current State: If null, make an empty object instead, unless Void, which is null.
            Z currentSerializedState = matsTrace.getCurrentState();

            S currentSto = (currentSerializedState == null
                    ? (stateClass != Void.class
                            ? _parentFactory.getMatsSerializer().newInstance(stateClass)
                            : null)
                    : _parentFactory.getMatsSerializer().deserializeObject(currentSerializedState, stateClass));

            // :: Current Call, incoming Message DTO
            Call<Z> currentCall = matsTrace.getCurrentCall();
            I incomingDto = _parentFactory.getMatsSerializer().deserializeObject(currentCall.getData(), incomingClass);

            double millisDeserializing = (System.nanoTime() - nanosStart) / 1_000_000d;

            log.info(LOG_PREFIX + "Unstashing message from [" + stash.length + " B] stash, R:[" + replyClass
                    .getSimpleName() + "], S:[" + stateClass.getSimpleName() + "], I:[" + incomingClass.getSimpleName()
                    + "]. From StageId:[" + matsTrace.getCurrentCall().getFrom() + "], This StageId:[" + stageId
                    + "], NextStageId:[" + nextStageId + "] - deserializing took ["
                    + millisDeserializing + " ms]");

            // :: Invoke the process lambda (the actual user code).
            try {
                lambda.process(new JmsMatsProcessContext<>(
                        _parentFactory,
                        endpointId,
                        stageId,
                        messageId,
                        nextStageId,
                        stash, zstartMatsTrace + 1, stash.length - zstartMatsTrace - 1,
                        matsTraceMeta, matsTrace,
                        currentSto, new LinkedHashMap<>(), new LinkedHashMap<>(),
                        _messagesToSend, _doAfterCommitRunnableHolder),
                        currentSto, incomingDto);
            }
            catch (MatsRefuseMessageException e) {
                throw new IllegalStateException("Cannot throw MatsRefuseMessageException when unstash()'ing!"
                        + " You should have done that when you first received the message, before"
                        + " stash()'ing it.", e);
            }
        }

        private static void validateByte(byte[] stash, int idx, int value) {
            if (stash[idx] != value) {
                throw new IllegalArgumentException("The stash bytes shall start with ASCII letters 'MATSjmts' and then"
                        + " a byte denoting the version. Index [" + idx + "] should be [" + value
                        + "], but was [" + stash[idx] + "].");
            }
        }

        private static int findZero(byte[] stash, int fromIndex) {
            try {
                int t = fromIndex;
                while (stash[t] != 0) {
                    t++;
                }
                return t;
            }
            catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException("The stash byte array does not contain the zeros I expected,"
                        + " starting from index [" + fromIndex + "]");
            }
        }

        private void copyOverAnyExistingTraceProperties(MatsTrace<Z> matsTrace) {
            // ?: Do we have an existing MatsTrace (implying that we are being initiated within a Stage)
            if (_existingMatsTrace != null) {
                // -> Yes, so copy over existing Trace Properties
                for (String key : _existingMatsTrace.getTracePropertyKeys()) {
                    matsTrace.setTraceProperty(key, _existingMatsTrace.getTraceProperty(key));
                }
            }
        }

        private void checkCommon(String msg) {
            if (_traceId == null) {
                throw new NullPointerException(msg + ": Missing 'traceId'.");
            }
            if (_from == null) {
                throw new NullPointerException(msg + ": Missing 'from'.");
            }
            if (_to == null) {
                throw new NullPointerException(msg + ": Missing 'to'.");
            }
        }

        @Override
        public String toString() {
            return idThis();
        }
    }
}
