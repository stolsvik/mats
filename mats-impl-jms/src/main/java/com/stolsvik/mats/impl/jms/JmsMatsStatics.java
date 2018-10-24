package com.stolsvik.mats.impl.jms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;

import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsSerializer.SerializedMatsTrace;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call.Channel;
import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;

public interface JmsMatsStatics {

    String LOG_PREFIX = "#JMATS# ";

    String THREAD_PREFIX = "MATS:";

    String TRACE_ID_KEY = "traceId";

    /**
     * Holds the entire contents of a "Mats Message" - so that it can be sent later.
     */
    class JmsMatsMessage<Z> {
        private final String _what;

        private final MatsTrace<Z> _matsTrace;

        private final Map<String, byte[]> _bytes;
        private final Map<String, String> _strings;

        private final SerializedMatsTrace _serializedOutgoingMatsTrace;

        private final double _totalProductionTimeMillis;

        public JmsMatsMessage(String what, MatsTrace<Z> matsTrace, Map<String, byte[]> bytes,
                Map<String, String> strings,
                SerializedMatsTrace serializedOutgoingMatsTrace, double totalProductionTimeMillis) {
            _what = what;
            _matsTrace = matsTrace;
            _bytes = bytes;
            _strings = strings;
            _serializedOutgoingMatsTrace = serializedOutgoingMatsTrace;
            _totalProductionTimeMillis = totalProductionTimeMillis;
        }

        public String getWhat() {
            return _what;
        }

        public MatsTrace<Z> getMatsTrace() {
            return _matsTrace;
        }

        public Map<String, byte[]> getBytes() {
            return _bytes;
        }

        public Map<String, String> getStrings() {
            return _strings;
        }

        public SerializedMatsTrace getSerializedOutgoingMatsTrace() {
            return _serializedOutgoingMatsTrace;
        }

        public double getTotalProductionTimeMillis() {
            return _totalProductionTimeMillis;
        }
    }

    /**
     * Common message enqueuing method - handles commonalities.
     *
     * <b>Notice that the props-, bytes- and Strings-Maps come back cleared.</b>
     */
    default <Z> JmsMatsMessage<Z> produceJmsMatsMessage(Logger log, long nanosStart,
            MatsSerializer<Z> serializer,
            MatsTrace<Z> outgoingMatsTrace,
            HashMap<String, Object> props,
            HashMap<String, byte[]> bytes,
            HashMap<String, String> strings, String what) {
        // :: Add the MatsTrace properties
        for (Entry<String, Object> entry : props.entrySet()) {
            outgoingMatsTrace.setTraceProperty(entry.getKey(), serializer.serializeObject(entry.getValue()));
        }
        // Clear the props-map
        props.clear();

        // Serialize the outgoing MatsTrace
        SerializedMatsTrace serializedOutgoingMatsTrace = serializer.serializeMatsTrace(outgoingMatsTrace);

        // :: Clone the bytes and strings Maps, and then clear the local Maps for any next message.
        @SuppressWarnings("unchecked")
        HashMap<String, byte[]> bytesCopied = (HashMap<String, byte[]>) bytes.clone();
        bytes.clear();
        @SuppressWarnings("unchecked")
        HashMap<String, String> stringsCopied = (HashMap<String, String>) strings.clone();
        strings.clear();

        double totalProductionTimeMillis = (System.nanoTime() - nanosStart) / 1_000_000d;

        // Produce the JmsMatsMessage
        JmsMatsMessage<Z> jmsMatsMessage = new JmsMatsMessage<>(what, outgoingMatsTrace, bytesCopied, stringsCopied,
                serializedOutgoingMatsTrace, totalProductionTimeMillis);

        // Log
        log.info(LOG_PREFIX + "PRODUCED [" + what + "] message to [" + outgoingMatsTrace.getCurrentCall().getTo()
                + "], MT->serialize:[" + serializedOutgoingMatsTrace.getSizeUncompressed()
                + " B, " + serializedOutgoingMatsTrace.getMillisSerialization()
                + " ms]->comp:[" + serializedOutgoingMatsTrace.getMeta()
                + " " + serializedOutgoingMatsTrace.getMillisCompression()
                + " ms]->final:[" + serializedOutgoingMatsTrace.getMatsTraceBytes().length
                + " B] - tot.prod.time w/DTO&STO:[" + totalProductionTimeMillis + " ms]");

        // Return masterpiece
        return jmsMatsMessage;
    }

    /**
     * Send a bunch of {@link JmsMatsMessage}s.
     */
    default <Z> void sendMatsMessages(Logger log, long nanosStart, Session jmsSession, JmsMatsFactory<Z> jmsMatsFactory,
            List<JmsMatsMessage<Z>> messagesToSend) throws JmsMatsJmsException {

        if (log.isDebugEnabled()) log.debug("Sending [" + messagesToSend.size() + "] messages.");
        // Create MessageProducer w/o specific Destination (will be given in .send(..))
        MessageProducer messageProducer;
        long nanosStartCreateProducer = System.nanoTime();
        try {
            messageProducer = jmsSession.createProducer(null);
        }
        catch (JMSException e) {
            throw new JmsMatsJmsException("Got problems creating a MessageProducer from Session [" + jmsSession + "].",
                    e);
        }
        long nanosStartSendingMessages = System.nanoTime();
        double millisCreateProducer = (nanosStartSendingMessages - nanosStartCreateProducer) / 1_000_000d;

        for (JmsMatsMessage<Z> jmsMatsMessage : messagesToSend) {
            Channel toChannel = jmsMatsMessage.getMatsTrace().getCurrentCall().getTo();
            try {
                long nanosStartSend = System.nanoTime();
                byte[] matsTraceBytes = jmsMatsMessage.getSerializedOutgoingMatsTrace().getMatsTraceBytes();

                // Get FactoryConfig
                FactoryConfig factoryConfig = jmsMatsFactory.getFactoryConfig();

                // Create the JMS Message that will be sent.
                MapMessage mm = jmsSession.createMapMessage();
                // Set the MatsTrace.
                mm.setBytes(factoryConfig.getMatsTraceKey(), matsTraceBytes);
                mm.setString(factoryConfig.getMatsTraceKey() + MatsSerializer.META_KEY_POSTFIX,
                        jmsMatsMessage.getSerializedOutgoingMatsTrace().getMeta());

                // :: Add the properties to the MapMessage
                for (Entry<String, byte[]> entry : jmsMatsMessage.getBytes().entrySet()) {
                    mm.setBytes(entry.getKey(), entry.getValue());
                }
                for (Entry<String, String> entry : jmsMatsMessage.getStrings().entrySet()) {
                    mm.setString(entry.getKey(), entry.getValue());
                }

                // :: Create the JMS Queue or Topic.
                Destination destination = toChannel.getMessagingModel() == MessagingModel.QUEUE
                        ? jmsSession.createQueue(factoryConfig.getMatsDestinationPrefix() + toChannel.getId())
                        : jmsSession.createTopic(factoryConfig.getMatsDestinationPrefix() + toChannel.getId());

                // TODO: OPTIMIZE: Use "asynchronous sends", i.e. register completion listeners (catch exceptions) and
                // close at the end.

                // TODO: OPTIMIZE: Is it worth it to cache producers?! Could do it on the JmsSessionHolder..

                // Setting DeliveryMode: NonPersistent or Persistent
                int deliveryMode = jmsMatsMessage.getMatsTrace().isNonPersistent()
                        ? DeliveryMode.NON_PERSISTENT
                        : DeliveryMode.PERSISTENT;
                // Setting Priority: 4 is default, 9 is highest.
                int priority = jmsMatsMessage.getMatsTrace().isInteractive() ? 9 : 4;
                // TODO: Set time-to-live
                // Send the message (but since transactional, won't be committed until TransactionContext does).
                messageProducer.send(destination, mm, deliveryMode, priority, 0);

                // Log it.
                double millisSent = (System.nanoTime() - nanosStartSend) / 1_000_000d;
                log.info(LOG_PREFIX + "SENDING [" + jmsMatsMessage.getWhat() + "] message to [" + destination
                        + "], send took:[" + millisSent + " ms] (production was:[" + jmsMatsMessage
                                .getTotalProductionTimeMillis() + "]).");
            }
            catch (JMSException e) {
                throw new JmsMatsJmsException("Got problems sending [" + jmsMatsMessage.getWhat() + "] to [" + toChannel
                        + "] via JMS API.", e);
            }
        }
        long nanosStartClosingProducer = System.nanoTime();
        double millisSendingMessags = (nanosStartClosingProducer - nanosStartCreateProducer) / 1_000_000d;

        try {
            messageProducer.close();
        }
        catch (JMSException e) {
            throw new JmsMatsJmsException("Got problems closing the MessageProducer [" + messageProducer
                    + "] from Session [" + jmsSession + "].", e);
        }

        long nanosFinal = System.nanoTime();
        double millisCloseProducer = (nanosFinal - nanosStartClosingProducer) / 1_000_000d;
        double millisTotal = (nanosFinal - nanosStart) / 1_000_000d;
        log.info(LOG_PREFIX + "SENT [" + messagesToSend.size() + "] messages: Creating producer:["
                + millisCreateProducer + "], sending messages:[" + millisSendingMessags + "], closing producer:["
                + millisCloseProducer + "] - total:[" + millisTotal + "].");
    }

    default String id(String what, Object obj) {
        return what + '@' + Integer.toHexString(System.identityHashCode(obj));
    }

    default String id(Object obj) {
        return id(obj.getClass().getSimpleName(), obj);
    }

    default String idThis() {
        return id(this);
    }

    default String stageOrInit(JmsMatsTxContextKey txContextKey) {
        if (txContextKey.getStage() != null) {
            return "StageProcessor for [" + txContextKey.getStage() + "]";
        }
        return "Initiation";
    }
}
