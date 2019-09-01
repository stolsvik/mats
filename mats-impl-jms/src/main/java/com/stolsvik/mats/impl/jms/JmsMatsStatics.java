package com.stolsvik.mats.impl.jms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.MDC;

import com.stolsvik.mats.MatsEndpoint.MatsObject;
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
    // Capitalization of JMSMessageID as they do in the JMS API.
    String MDC_JMS_MESSAGE_ID_IN = "mats.JMSMessageID.In";
    String MDC_MATS_MESSAGE_ID_IN = "mats.MatsMessageId.In";
    String MDC_JMS_MESSAGE_ID_OUT = "mats.JMSMessageID.Out";
    String MDC_MATS_MESSAGE_ID_OUT = "mats.MatsMessageId.Out";
    String MDC_MATS_INCOMING = "mats.Incoming";
    String MDC_MATS_INITIATE = "mats.Initiate";
    String MDC_MATS_OUTGOING = "mats.Outgoing";
    String MDC_MATS_FROM = "mats.From";
    String MDC_MATS_TO = "mats.To";
    String MDC_MATS_AUDIT = "mats.Audit";

    String JMS_MSG_PROP_FROM = MDC_MATS_FROM;
    String JMS_MSG_PROP_TO = MDC_MATS_TO;
    String JMS_MSG_PROP_NO_AUDIT = "mats.NoAudit";
    String JMS_MSG_PROP_MATS_MSG_ID = "mats.MatsMsgId";
    String JMS_MSG_PROP_TRACE_ID = "mats.TraceId";

    /**
     * Number of milliseconds to "extra wait" after timeoutMillis or gracefulShutdownMillis is gone.
     */
    int EXTRA_GRACE_MILLIS = 50;

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
        try { // :: try-finally: Restore MDC
            MDC.put(MDC_TRACE_ID, outgoingMatsTrace.getTraceId());
            MDC.put(MDC_MATS_MESSAGE_ID_OUT, outgoingMatsTrace.getCurrentCall().getMatsMessageId());
            MDC.put(MDC_MATS_OUTGOING, "true");
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
            MDC.remove(MDC_MATS_MESSAGE_ID_OUT);
            // Outgoing
            MDC.remove(MDC_MATS_OUTGOING);
        }
    }

    /**
     * Send a bunch of {@link JmsMatsMessage}s.
     */
    default <Z> void sendMatsMessages(Logger log, long nanosStart, JmsSessionHolder jmsSessionHolder,
            JmsMatsFactory<Z> jmsMatsFactory, List<JmsMatsMessage<Z>> messagesToSend) throws JmsMatsJmsException {
        try { // :: try-finally: Remove MDC_MATS_OUTGOING
            MDC.put(MDC_MATS_OUTGOING, "true");
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
                try { // :: try-finally: Restore MDC
                    MatsTrace<Z> outgoingMatsTrace = jmsMatsMessage.getMatsTrace();
                    // Set MDC for this outgoing message
                    MDC.put(MDC_TRACE_ID, outgoingMatsTrace.getTraceId());
                    MDC.put(MDC_MATS_MESSAGE_ID_OUT, outgoingMatsTrace.getCurrentCall().getMatsMessageId());
                    MDC.put(MDC_MATS_FROM, outgoingMatsTrace.getCurrentCall().getFrom());
                    MDC.put(MDC_MATS_TO, toChannel.getId());
                    MDC.put(MDC_MATS_AUDIT, "" + (!outgoingMatsTrace.isNoAudit()));
                    byte[] matsTraceBytes = jmsMatsMessage.getSerializedOutgoingMatsTrace().getMatsTraceBytes();

                    // Get FactoryConfig
                    FactoryConfig factoryConfig = jmsMatsFactory.getFactoryConfig();

                    // Create the JMS Message that will be sent.
                    MapMessage mm = jmsSession.createMapMessage();
                    // Set the MatsTrace.
                    mm.setBytes(factoryConfig.getMatsTraceKey(), matsTraceBytes);
                    mm.setString(factoryConfig.getMatsTraceKey() + MatsSerializer.META_KEY_POSTFIX,
                            jmsMatsMessage.getSerializedOutgoingMatsTrace().getMeta());

                    // :: Add the Mats properties to the MapMessage
                    for (Entry<String, byte[]> entry : jmsMatsMessage.getBytes().entrySet()) {
                        mm.setBytes(entry.getKey(), entry.getValue());
                    }
                    for (Entry<String, String> entry : jmsMatsMessage.getStrings().entrySet()) {
                        mm.setString(entry.getKey(), entry.getValue());
                    }

                    // :: Add some JMS Properties to simplify logging on MQ
                    mm.setStringProperty(JMS_MSG_PROP_TRACE_ID, outgoingMatsTrace.getTraceId());
                    mm.setStringProperty(JMS_MSG_PROP_MATS_MSG_ID, outgoingMatsTrace.getCurrentCall()
                            .getMatsMessageId());
                    mm.setStringProperty(JMS_MSG_PROP_FROM, outgoingMatsTrace.getCurrentCall().getFrom());
                    mm.setStringProperty(JMS_MSG_PROP_TO, toChannel.getId());
                    if (outgoingMatsTrace.isNoAudit()) {
                        mm.setBooleanProperty(JMS_MSG_PROP_NO_AUDIT, true);
                    }

                    // Setting DeliveryMode: NonPersistent or Persistent
                    int deliveryMode = outgoingMatsTrace.isNonPersistent()
                            ? DeliveryMode.NON_PERSISTENT
                            : DeliveryMode.PERSISTENT;

                    // Setting Priority: 4 is default, 9 is highest.
                    int priority = outgoingMatsTrace.isInteractive() ? 9 : 4;

                    // Get Time-To-Live
                    long timeToLive = outgoingMatsTrace.getTimeToLive();

                    // :: Create the JMS Queue or Topic.
                    // TODO: OPTIMIZE: Cache these?!
                    Destination destination = toChannel.getMessagingModel() == MessagingModel.QUEUE
                            ? jmsSession.createQueue(factoryConfig.getMatsDestinationPrefix() + toChannel.getId())
                            : jmsSession.createTopic(factoryConfig.getMatsDestinationPrefix() + toChannel.getId());

                    // TODO: OPTIMIZE: Use "asynchronous sends", i.e. register completion listeners (catch exceptions)
                    // and close at the end.

                    // :: Send the message (but since transactional, won't be committed until TransactionContext does).
                    messageProducer.send(destination, mm, deliveryMode, priority, timeToLive);

                    // We now have a JMSMessageID, so set it on MDC for outgoing.
                    MDC.put(MDC_JMS_MESSAGE_ID_OUT, mm.getJMSMessageID());

                    // Log it.
                    long nanosAtSent = System.nanoTime();
                    double millisSend = (nanosAtSent - nanosStartSend) / 1_000_000d;
                    log.info(LOG_PREFIX + "SENT [" + jmsMatsMessage.getWhat() + "] message to ["
                            + jmsMatsFactory.getFactoryConfig().getName() + "|" + destination
                            + "], msg creation + send took:[" + ms3(millisSend) + " ms] (production was:["
                            + ms3(jmsMatsMessage.getTotalProductionTimeMillis()) + " ms])"
                            + (messagesToSend.size() == 1
                                    ? ", total since recv/init:[" + ms3((nanosAtSent - nanosStart) / 1_000_000d)
                                            + " ms]."
                                    : "."));
                }
                catch (JMSException e) {
                    throw new JmsMatsJmsException("Got problems sending [" + jmsMatsMessage.getWhat()
                            + "] to [" + toChannel + "] via JMS API.", e);
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
                    MDC.remove(MDC_JMS_MESSAGE_ID_OUT);
                    // MatsMessageId
                    MDC.remove(MDC_MATS_MESSAGE_ID_OUT);
                    MDC.remove(MDC_MATS_FROM);
                    MDC.remove(MDC_MATS_TO);
                    MDC.remove(MDC_MATS_AUDIT);
                }
            }
            // Only log tally-line if we sent more than one message
            if (messagesToSend.size() > 1) {
                long nanosFinal = System.nanoTime();
                double millisSendingMessags = (nanosFinal - nanosStartSendingMessages) / 1_000_000d;
                double millisTotal = (nanosFinal - nanosStart) / 1_000_000d;
                log.info(LOG_PREFIX + "SENT TOTAL [" + messagesToSend.size() + "] messages, took:[" + ms3(
                        millisSendingMessags)
                        + "] - total since recv/init:[" + ms3(millisTotal) + "].");
            }
        }
        finally {
            // :: Clean MDC: Outgoing
            MDC.remove(MDC_MATS_OUTGOING);
        }
    }

    default <I, Z> I handleIncomingMessageMatsObject(MatsSerializer<Z> matsSerializer, Class<I> incomingMessageClass,
            Z data) {
        @SuppressWarnings(value = "unchecked") // We check that I is indeed MatsObject
        I incomingDto = incomingMessageClass != MatsObject.class
                ? matsSerializer.deserializeObject(data, incomingMessageClass)
                : (I) new MatsObject() {
                    @Override
                    public <T> T toClass(Class<T> type) throws IllegalArgumentException {
                        try {
                            return matsSerializer.deserializeObject(data, type);
                        }
                        catch (Throwable t) {
                            throw new IllegalArgumentException("Could not deserialize the data"
                                    + " contained in MatsObject to class [" + type.getName()
                                    + "].");
                        }
                    }
                };
        return incomingDto;
    }

    // 62 points in this alphabeth
    String RANDOM_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    /**
     * @param length
     *            the desired length of the returned random string.
     * @return a random string of the specified length.
     */
    default String randomString(int length) {
        StringBuilder buf = new StringBuilder(length);
        ThreadLocalRandom tlRandom = ThreadLocalRandom.current();
        for (int i = 0; i < length; i++)
            buf.append(RANDOM_ALPHABET.charAt(tlRandom.nextInt(RANDOM_ALPHABET.length())));
        return buf.toString();
    }

    default String createFlowId(long creationTimeMillis) {
        // 2^122 = 5316911983139663491615228241121378304 // "type 4 (random) UUID"
        // 62^20 = 704423425546998022968330264616370176 // This ID
        // Feels good enough. One more letter would have totally topped it, but this is way too much already.
        return "m_" + randomString(20) + "_T" + Long.toUnsignedString(creationTimeMillis, 36);
    }

    default String createMatsMessageId(String flowId, long matsTraceCreationMillis, long messageCreationMillis,
            int callNumber) {
        // Since we can have clock skews between servers, and we do not want a "-" in the messageId (due to the
        // double-clickableness mentioned below), we make -10 -> "n10".
        long millisSince = messageCreationMillis - matsTraceCreationMillis;
        String millisSinceString = millisSince >= 0 ? Long.toString(millisSince) : "n" + Math.abs(millisSince);
        // A MatsMessageId ends up looking like this: 'm_XBExAa1iioAGFVRk6nR5_Tjzswm4ys_t49_n22'
        // Or for negative millisSince: 'm_XBExAa1iioAGFVRk6nR5_Tjzswm4ys_tn49_n22'
        // NOTICE FEATURE: You can double-click anywhere inside that string, and get the entire id marked! w00t!
        return flowId + "_t" + millisSinceString + "_n" + callNumber;
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
