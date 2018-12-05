package com.stolsvik.mats.impl.jms;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.impl.jms.JmsMatsInitiator.JmsMatsInitiate;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call;
import com.stolsvik.mats.serial.MatsTrace.Call.Channel;
import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;

/**
 * The JMS MATS implementation of {@link ProcessContext}. Instantiated for each incoming JMS message that is processed,
 * given to the {@link MatsStage}'s process lambda.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsProcessContext<R, S, Z> implements ProcessContext<R>, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsProcessContext.class);

    // private final JmsMatsStage<R, ?, ?, Z> _matsStage;

    private final JmsMatsFactory<Z> _parentFactory;

    private final String _endpointId;
    private final String _stageId;
    private final String _messageId;
    private final String _nextStageId;

    private final byte[] _incomingSerializedMatsTrace;
    private final int _mtSerOffset;
    private final int _mtSerLength;  // The reason for having this separate, is when unstashing: Length != entire thing.
    private final String _incomingSerializedMatsTraceMeta;
    private final MatsTrace<Z> _incomingMatsTrace;
    private final LinkedHashMap<String, byte[]> _incomingBinaries;
    private final LinkedHashMap<String, String> _incomingStrings;
    private final S _incomingAndOutgoingState;
    private final List<JmsMatsMessage<Z>> _messagesToSend;

    JmsMatsProcessContext(JmsMatsFactory<Z> parentFactory,
            String endpointId,
            String stageId,
            String messageId,
            String nextStageId,
            byte[] incomingSerializedMatsTrace, int mtSerOffset, int mtSerLength,
            String incomingSerializedMatsTraceMeta,
            MatsTrace<Z> incomingMatsTrace, S incomingAndOutgoingState,
            LinkedHashMap<String, byte[]> incomingBinaries, LinkedHashMap<String, String> incomingStrings,
            List<JmsMatsMessage<Z>> out_messagesToSend) {
        _parentFactory = parentFactory;

        _endpointId = endpointId;
        _stageId = stageId;
        _messageId = messageId;
        _nextStageId = nextStageId;

        _incomingSerializedMatsTrace = incomingSerializedMatsTrace;
        _mtSerOffset = mtSerOffset;
        _mtSerLength = mtSerLength;
        _incomingSerializedMatsTraceMeta = incomingSerializedMatsTraceMeta;
        _incomingMatsTrace = incomingMatsTrace;
        _incomingBinaries = incomingBinaries;
        _incomingStrings = incomingStrings;
        _incomingAndOutgoingState = incomingAndOutgoingState;
        _messagesToSend = out_messagesToSend;
    }

    private final LinkedHashMap<String, Object> _outgoingProps = new LinkedHashMap<>();
    private final LinkedHashMap<String, byte[]> _outgoingBinaries = new LinkedHashMap<>();
    private final LinkedHashMap<String, String> _outgoingStrings = new LinkedHashMap<>();

    @Override
    public String getStageId() {
        return _stageId;
    }

    @Override
    public String getFromStageId() {
        return _incomingMatsTrace.getCurrentCall().getFrom();
    }

    @Override
    public String getMessageId() {
        return _messageId;
    }

    @Override
    public boolean isNonPersistent() {
        return _incomingMatsTrace.isNonPersistent();
    }

    @Override
    public boolean isInteractive() {
        return _incomingMatsTrace.isInteractive();
    }

    @Override
    public String toString() {
        return _incomingMatsTrace.toString();
    }

    @Override
    public String getTraceId() {
        return _incomingMatsTrace.getTraceId();
    }

    @Override
    public String getEndpointId() {
        return _endpointId;
    }

    @Override
    public byte[] getBytes(String key) {
        return _incomingBinaries.get(key);
    }

    @Override
    public String getString(String key) {
        return _incomingStrings.get(key);
    }

    @Override
    public void addBytes(String key, byte[] payload) {
        _outgoingBinaries.put(key, payload);
    }

    @Override
    public void addString(String key, String payload) {
        _outgoingStrings.put(key, payload);
    }

    @Override
    public void setTraceProperty(String propertyName, Object propertyValue) {
        _outgoingProps.put(propertyName, propertyValue);
    }

    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    static final byte[] NO_NEXT_STAGE = "-".getBytes(CHARSET_UTF8);

    @Override
    public byte[] stash() {
        long nanosStart = System.nanoTime();

        // Serialize the endpointId
        byte[] b_endpointId = _endpointId.getBytes(CHARSET_UTF8);
        // .. stageId
        byte[] b_stageId = _stageId.getBytes(CHARSET_UTF8);
        // .. nextStageId, handling that it might be null.
        byte[] b_nextStageId = _nextStageId == null ? NO_NEXT_STAGE : _nextStageId.getBytes(CHARSET_UTF8);
        // .. serialized MatsTrace's meta info:
        byte[] b_meta = _incomingSerializedMatsTraceMeta.getBytes(CHARSET_UTF8);
        // .. messageId
        byte[] b_messageId = _messageId.getBytes(CHARSET_UTF8);

        // :: Create and fill the Stash resulting array

        // Total length:
        // . 8 for the 2 x FourCC's "MATSjmts"
        // + 1 for the version, '1'
        // + 1 for the number of zeros, currently 6.

        // + 1 for the 0-terminator  // NOTICE: Can add more future data between n.o.Zeros and this zero-delimiter.
        // + b_endpointId.length
        // + 1 for the 0-terminator
        // + b_stageId.length
        // + 1 for the 0-terminator
        // + b_nextStageId.length
        // + 1 for the 0-terminator
        // + b_meta.length
        // + 1 for the 0-terminator
        // + b_messageId.length
        // + 1 for the 0-terminator
        // + length of incoming serialized MatsTrace, _mtSerLength

        int fullStashLength = 8
                + 1
                + 1

                + 1 + b_endpointId.length
                + 1 + b_stageId.length
                + 1 + b_nextStageId.length
                + 1 + b_meta.length
                + 1 + b_messageId.length
                + 1 + _mtSerLength;
        byte[] fullStash = new byte[fullStashLength];
        // "MATSjmts":
        // * "MATS" as FourCC/"Magic Number", per spec.
        // * "jmts" for "Jms MatsTrace Serializer": This is the JMS impl of Mats, which employs MatsTraceSerializer.
        fullStash[0] = 77; // M
        fullStash[1] = 65; // A
        fullStash[2] = 84; // T
        fullStash[3] = 83; // S
        fullStash[4] = 106; // j
        fullStash[5] = 109; // m
        fullStash[6] = 116; // t
        fullStash[7] = 115; // s
        fullStash[8] = 1; // Version -- NOTICE! that there are room to add more stuff here before first 0-byte.
        fullStash[9] = 6; // Number of zeros - to be able to add stuff later, and have older deserializers handle it.
        // ZERO 1: All bytes in new initialized array is 0 already
        // EndpointId:
        int startPos_EndpointId = 8 + 1 + 1 + 1;
        System.arraycopy(b_endpointId, 0, fullStash, startPos_EndpointId, b_endpointId.length);
        // ZERO 2: All bytes in new initialized array is 0 already
        // StageId:
        int startPos_StageId = startPos_EndpointId + b_endpointId.length + 1;
        System.arraycopy(b_stageId, 0, fullStash, startPos_StageId, b_stageId.length);
        // ZERO 3: All bytes in new initialized array is 0 already
        // NextStageId:
        int startPos_NextStageId = startPos_StageId + b_stageId.length + 1;
        System.arraycopy(b_nextStageId, 0, fullStash, startPos_NextStageId, b_nextStageId.length);
        // ZERO 4: All bytes in new initialized array is 0 already
        // Meta:
        int startPos_Meta = startPos_NextStageId + b_nextStageId.length + 1;
        System.arraycopy(b_meta, 0, fullStash, startPos_Meta, b_meta.length);
        // ZERO 5: All bytes in new initialized array is 0 already
        // MessageId:
        int startPos_MessageId = startPos_Meta + b_meta.length + 1;
        System.arraycopy(b_messageId, 0, fullStash, startPos_MessageId, b_messageId.length);
        // ZERO 6: All bytes in new initialized array is 0 already
        // Actual Serialized MatsTrace:
        int startPos_MatsTrace = startPos_MessageId + b_messageId.length + 1;
        System.arraycopy(_incomingSerializedMatsTrace, _mtSerOffset,
                fullStash, startPos_MatsTrace, _mtSerLength);

        double millisSerializing = (System.nanoTime() - nanosStart) / 1_000_000d;

        log.info(LOG_PREFIX + "Stashed Mats flow, stash:[" + fullStash.length + " B], serializing took:["
                + millisSerializing + " ms].");

        return fullStash;
    }

    @Override
    public <T> T getTraceProperty(String propertyName, Class<T> clazz) {
        Z value = _incomingMatsTrace.getTraceProperty(propertyName);
        if (value == null) {
            return null;
        }
        return _parentFactory.getMatsSerializer().deserializeObject(value, clazz);
    }

    @Override
    public void request(String endpointId, Object requestDto) {
        long nanosStart = System.nanoTime();
        // :: Assert that we have a next-stage
        if (_nextStageId == null) {
            throw new IllegalStateException("Stage [" + _stageId
                    + "] invoked context.request(..), but there is no next stage to reply to."
                    + " Use context.send(..) if you want to 'invoke' the endpoint w/o req/rep semantics.");
        }
        // :: Create next MatsTrace
        MatsSerializer<Z> matsSerializer = _parentFactory.getMatsSerializer();
        MatsTrace<Z> requestMatsTrace = _incomingMatsTrace.addRequestCall(_stageId,
                endpointId, MessagingModel.QUEUE,
                _nextStageId, MessagingModel.QUEUE,
                matsSerializer.serializeObject(requestDto),
                matsSerializer.serializeObject(_incomingAndOutgoingState), null);

        // TODO: Add debug info!
        requestMatsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                _parentFactory.getFactoryConfig().getAppVersion(),
                _parentFactory.getFactoryConfig().getNodename(), System.currentTimeMillis(), "Callalala!");

        // Produce the REQUEST JmsMatsMessage to send
        JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory.getMatsSerializer(),
                requestMatsTrace, _outgoingProps, _outgoingBinaries, _outgoingStrings, "REQUEST");
        _messagesToSend.add(request);
    }

    @Override
    public void reply(Object replyDto) {
        long nanosStart = System.nanoTime();
        // :: Short-circuit the reply (to no-op) if there is nothing on the stack to reply to.
        List<Channel> stack = _incomingMatsTrace.getCurrentCall().getStack();
        if (stack.size() == 0) {
            // This is OK, it is just like a normal java call where you do not use the return value, e.g. map.put(k, v).
            // It happens if you use "send" (aka "fire-and-forget") to an endpoint which has reply-semantics, which
            // is legal.
            log.info("Stage [" + _stageId + " invoked context.reply(..), but there are no elements"
                    + " on the stack, hence no one to reply to, ignoring.");
            return;
        }

        // :: Create next MatsTrace
        MatsSerializer<Z> matsSerializer = _parentFactory.getMatsSerializer();
        MatsTrace<Z> replyMatsTrace = _incomingMatsTrace.addReplyCall(_stageId,
                matsSerializer.serializeObject(replyDto));

        // TODO: Add debug info!
        Call<Z> currentCall = replyMatsTrace.getCurrentCall();
        currentCall.setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                _parentFactory.getFactoryConfig().getAppVersion(),
                _parentFactory.getFactoryConfig().getNodename(), System.currentTimeMillis(), "Callalala!");

        // Produce the REPLY JmsMatsMessage to send
        JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory.getMatsSerializer(),
                replyMatsTrace, _outgoingProps, _outgoingBinaries, _outgoingStrings, "REPLY");
        _messagesToSend.add(request);
    }

    @Override
    public void next(Object incomingDto) {
        long nanosStart = System.nanoTime();
        // :: Assert that we have a next-stage
        if (_nextStageId == null) {
            throw new IllegalStateException("Stage [" + _stageId
                    + "] invoked context.next(..), but there is no next stage.");
        }

        // :: Create next (heh!) MatsTrace
        MatsSerializer<Z> matsSerializer = _parentFactory.getMatsSerializer();
        MatsTrace<Z> nextMatsTrace = _incomingMatsTrace.addNextCall(_stageId, _nextStageId,
                matsSerializer.serializeObject(incomingDto), matsSerializer.serializeObject(_incomingAndOutgoingState));

        // TODO: Add debug info!
        nextMatsTrace.getCurrentCall().setDebugInfo(_parentFactory.getFactoryConfig().getAppName(),
                _parentFactory.getFactoryConfig().getAppVersion(),
                _parentFactory.getFactoryConfig().getNodename(), System.currentTimeMillis(), "Callalala!");

        // Produce the NEXT JmsMatsMessage to send
        JmsMatsMessage<Z> request = produceJmsMatsMessage(log, nanosStart, _parentFactory.getMatsSerializer(),
                nextMatsTrace, _outgoingProps, _outgoingBinaries, _outgoingStrings, "NEXT");
        _messagesToSend.add(request);
    }

    @Override
    public void initiate(InitiateLambda lambda) {
        lambda.initiate(new JmsMatsInitiate<>(_parentFactory, _messagesToSend, _incomingMatsTrace, _outgoingProps));
    }
}
