package com.stolsvik.mats.impl.jms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.MDC;

import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsSerializer.SerializedMatsTrace;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.MatsTrace.Call.Channel;
import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;

public interface JmsMatsStatics {

    String LOG_PREFIX = "#JMATS# ";

    String THREAD_PREFIX = "MATS:";

    // Not using "mats." prefix for "traceId", as it is hopefully generic yet specific
    // enough that it might be used in similar applications.
    String MDC_TRACE_ID = "traceId";
    // Using "mats." prefix for the more Mats-specific parts of MDC
    String MDC_JMS_MESSAGE_ID = "mats.JMSMessageId";
    String MDC_MATS_MESSAGE_ID = "mats.MatsMessageId";

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
     * Common message production method - handles commonalities.
     *
     * <b>Notice that the props-, bytes- and Strings-Maps come back cleared.</b>
     */
    default <Z> JmsMatsMessage<Z> produceJmsMatsMessage(Logger log, long nanosStart,
            MatsSerializer<Z> serializer,
            MatsTrace<Z> outgoingMatsTrace,
            HashMap<String, Object> props,
            HashMap<String, byte[]> bytes,
            HashMap<String, String> strings, String what, String matsFactoryName) {
        String existingTraceId = MDC.get(MDC_TRACE_ID);
        String existingMatsMessageId = MDC.get(MDC_MATS_MESSAGE_ID);
        try {
            MDC.put(MDC_TRACE_ID, outgoingMatsTrace.getTraceId());
            MDC.put(MDC_MATS_MESSAGE_ID, outgoingMatsTrace.getCurrentCall().getMatsMessageId());
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
            log.info(LOG_PREFIX + "PRODUCED [" + what + "] message to [" + matsFactoryName + "|"
                    + outgoingMatsTrace.getCurrentCall().getTo()
                    + "], MT->serialize:[" + serializedOutgoingMatsTrace.getSizeUncompressed()
                    + " B, " + ms3(serializedOutgoingMatsTrace.getMillisSerialization())
                    + " ms]->comp:[" + serializedOutgoingMatsTrace.getMeta()
                    + " " + ms3(serializedOutgoingMatsTrace.getMillisCompression())
                    + " ms]->final:[" + serializedOutgoingMatsTrace.getMatsTraceBytes().length
                    + " B] - tot.prod.time w/DTO&STO:[" + ms3(totalProductionTimeMillis) + " ms]");

            // Return masterpiece
            return jmsMatsMessage;
        }
        finally {
            // :: Restore MDC
            // TraceId
            if (existingTraceId == null) {
                MDC.remove(MDC_TRACE_ID);
            }
            else {
                MDC.put(MDC_TRACE_ID, existingTraceId);
            }
            // MatsMessageId
            if (existingMatsMessageId == null) {
                MDC.remove(MDC_MATS_MESSAGE_ID);
            }
            else {
                MDC.put(MDC_MATS_MESSAGE_ID, existingMatsMessageId);
            }
        }
    }

    /**
     * Send a bunch of {@link JmsMatsMessage}s.
     */
    default <Z> void sendMatsMessages(Logger log, long nanosStart, JmsSessionHolder jmsSessionHolder,
            JmsMatsFactory<Z> jmsMatsFactory, List<JmsMatsMessage<Z>> messagesToSend) throws JmsMatsJmsException {
        if (messagesToSend.isEmpty()) {
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "No messages to send.");
            return;
        }
        Session jmsSession = jmsSessionHolder.getSession();
        if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Sending [" + messagesToSend.size() + "] messages.");

        MessageProducer messageProducer = jmsSessionHolder.getDefaultNoDestinationMessageProducer();

        long nanosStartSendingMessages = System.nanoTime();
        for (JmsMatsMessage<Z> jmsMatsMessage : messagesToSend) {
            long nanosStartSend = System.nanoTime();
            Channel toChannel = jmsMatsMessage.getMatsTrace().getCurrentCall().getTo();
            // :: Keep MDC to restore
            String existingTraceId = MDC.get(MDC_TRACE_ID);
            String existingMatsMessageId = MDC.get(MDC_MATS_MESSAGE_ID);
            String existingJmsMessageId = MDC.get(MDC_JMS_MESSAGE_ID);
            try {
                MatsTrace<Z> outgoingMatsTrace = jmsMatsMessage.getMatsTrace();
                // Set MDC for this outgoing message
                MDC.put(MDC_TRACE_ID, outgoingMatsTrace.getTraceId());
                MDC.put(MDC_MATS_MESSAGE_ID, outgoingMatsTrace.getCurrentCall().getMatsMessageId());
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

                // Setting DeliveryMode: NonPersistent or Persistent
                int deliveryMode = outgoingMatsTrace.isNonPersistent()
                        ? DeliveryMode.NON_PERSISTENT
                        : DeliveryMode.PERSISTENT;
                // Setting Priority: 4 is default, 9 is highest.
                int priority = outgoingMatsTrace.isInteractive() ? 9 : 4;
                // TODO: Set time-to-live (Issue #23)
                // Send the message (but since transactional, won't be committed until TransactionContext does).
                messageProducer.send(destination, mm, deliveryMode, priority, 0);
                // We now have a JMSMessageID, so set it on MDC for outgoing.
                MDC.put(MDC_JMS_MESSAGE_ID, mm.getJMSMessageID());

                // Log it.
                double millisSend = (System.nanoTime() - nanosStartSend) / 1_000_000d;
                log.info(LOG_PREFIX + "SENDING [" + jmsMatsMessage.getWhat() + "] message to ["
                        + jmsMatsFactory.getFactoryConfig().getName() + "|" + destination
                        + "], send took:[" + ms3(millisSend) + " ms] (production was:[" + ms3(jmsMatsMessage
                                .getTotalProductionTimeMillis()) + " ms]).");
            }
            catch (JMSException e) {
                throw new JmsMatsJmsException("Got problems sending [" + jmsMatsMessage.getWhat() + "] to [" + toChannel
                        + "] via JMS API.", e);
            }
            finally {
                // :: Restore MDC
                // TraceId
                if (existingTraceId == null) {
                    MDC.remove(MDC_TRACE_ID);
                }
                else {
                    MDC.put(MDC_TRACE_ID, existingTraceId);
                }
                // JMSMessageId
                if (existingJmsMessageId == null) {
                    MDC.remove(MDC_JMS_MESSAGE_ID);
                }
                else {
                    MDC.put(MDC_JMS_MESSAGE_ID, existingJmsMessageId);
                }
                // MatsMessageId
                if (existingMatsMessageId == null) {
                    MDC.remove(MDC_MATS_MESSAGE_ID);
                }
                else {
                    MDC.put(MDC_MATS_MESSAGE_ID, existingMatsMessageId);
                }
            }
        }
        long nanosFinal = System.nanoTime();
        double millisSendingMessags = (nanosFinal - nanosStartSendingMessages) / 1_000_000d;

        double millisTotal = (nanosFinal - nanosStart) / 1_000_000d;
        log.info(LOG_PREFIX + "SENT [" + messagesToSend.size() + "] messages, took:[" + ms3(millisSendingMessags)
                + "] - total since recv/init:[" + ms3(millisTotal) + "].");
    }

    default String createMatsMessageId() {
        Random random = new Random();
        return "mats_" + Long.toUnsignedString(System.currentTimeMillis(), 36)
                + "_" + Long.toUnsignedString(random.nextLong(), 36)
                + Long.toUnsignedString(random.nextLong(), 36);
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

    /**
     * Truncate milliseconds to 3 decimals.
     */
    default double ms3(double ms) {
        return Math.round(ms * 1000d) / 1000d;
    }
}
