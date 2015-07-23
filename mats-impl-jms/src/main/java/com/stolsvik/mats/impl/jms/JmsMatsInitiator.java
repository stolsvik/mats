package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.exceptions.MatsConnectionException;
import com.stolsvik.mats.util.MatsStringSerializer;

public class JmsMatsInitiator implements MatsInitiator {
    private final JmsMatsFactory _parentFactory;
    private final Connection _jmsConnection;
    private final MatsStringSerializer _matsJsonSerializer;

    public JmsMatsInitiator(JmsMatsFactory parentFactory, Connection jmsConnection,
            MatsStringSerializer matsJsonSerializer) {
        _parentFactory = parentFactory;
        _jmsConnection = jmsConnection;
        _matsJsonSerializer = matsJsonSerializer;
    }

    @Override
    public void initiate(InitiateLambda lambda) {
        Session session;
        try {
            session = _jmsConnection.createSession(true, 0);
        }
        catch (JMSException e) {
            throw new MatsConnectionException(
                    "Got a JMS Exception when trying to createSession(true, 0) on the JMS Connection [" + _jmsConnection
                            + "].", e);
        }
        lambda.initiate(new JmsMatsInitiate(_parentFactory, session, _matsJsonSerializer));
    }

    @Override
    public void close() {
        try {
            _jmsConnection.close();
        }
        catch (JMSException e) {
            throw new MatsConnectionException("Problems closing JMS Connection [" + _jmsConnection + "] from [" + this
                    + "].", e);
        }
    }
}
