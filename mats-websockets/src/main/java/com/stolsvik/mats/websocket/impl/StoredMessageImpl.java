package com.stolsvik.mats.websocket.impl;

import com.stolsvik.mats.websocket.impl.DefaultMatsSocketServer.ClusterStoreAndForward.StoredMessage;

/**
 * @author Endre Stølsvik 2019-12-07 23:29 - http://stolsvik.com/, endre@stolsvik.com
 */
public class StoredMessageImpl implements StoredMessage {

    private final long _id;
    int _deliveryAttempt;
    private final long _storedTimestamp;

    private final String _type;
    private final String _traceId;
    private final String _envelopeJson;

    public StoredMessageImpl(long id, int deliveryAttempt, long storedTimestamp, String type, String traceId,
            String envelopeJson) {
        _id = id;
        _deliveryAttempt = deliveryAttempt;
        _storedTimestamp = storedTimestamp;
        _type = type;
        _traceId = traceId;
        _envelopeJson = envelopeJson;
    }

    @Override
    public long getId() {
        return _id;
    }

    @Override
    public int getDeliveryAttempt() {
        return _deliveryAttempt;
    }

    @Override
    public long getStoredTimestamp() {
        return _storedTimestamp;
    }

    @Override
    public String getType() {
        return _type;
    }

    @Override
    public String getTraceId() {
        return _traceId;
    }

    @Override
    public String getEnvelopeJson() {
        return _envelopeJson;
    }
}
