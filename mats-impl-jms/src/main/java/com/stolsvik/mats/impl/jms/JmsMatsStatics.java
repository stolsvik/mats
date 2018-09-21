package com.stolsvik.mats.impl.jms;

import java.io.BufferedInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;
import com.stolsvik.mats.impl.jms.JmsMatsTransactionManager.JmsMatsTxContextKey;
import com.stolsvik.mats.serial.MatsTrace.Call;
import com.stolsvik.mats.serial.MatsTrace.Call.Channel;
import com.stolsvik.mats.serial.MatsTrace.Call.MessagingModel;
import org.slf4j.Logger;

import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsSerializer.SerializedMatsTrace;
import com.stolsvik.mats.serial.MatsTrace;

public interface JmsMatsStatics {

    String LOG_PREFIX = "#JMATS# ";

    String THREAD_PREFIX = "MATS:";

    String TRACE_ID_KEY = "traceId";

    /**
     * Common sending method - handles commonalities. <b>Notice that the bytes- and Strings-Maps come back cleared, even
     * if any part of sending throws.</b>
     */
    default <Z> void sendMatsMessage(Logger log, long nanosStart, Session jmsSession, JmsMatsFactory<Z> jmsMatsFactory,
            MatsTrace<Z> matsTrace, HashMap<String, Object> props,
            HashMap<String, byte[]> bytes, HashMap<String, String> strings,
            String what) {
        Channel toChannel = matsTrace.getCurrentCall().getTo();
        try {
            // :: Clone the bytes and strings Maps, and then clear the local Maps for any next message.
            // This could be done after the sending (the map shall be empty for every new message), but then if the
            // sending raises any RTE, it will not happen - and the user could potentially catch this, and send a new
            // message.
            @SuppressWarnings("unchecked")
            HashMap<String, Object> propsCopied = (HashMap<String, Object>) props.clone();
            props.clear();
            @SuppressWarnings("unchecked")
            HashMap<String, byte[]> bytesCopied = (HashMap<String, byte[]>) bytes.clone();
            bytes.clear();
            @SuppressWarnings("unchecked")
            HashMap<String, String> stringsCopied = (HashMap<String, String>) strings.clone();
            strings.clear();

            // Get the serializer
            MatsSerializer<Z> serializer = jmsMatsFactory.getMatsSerializer();

            // :: Add the MatsTrace properties
            for (Map.Entry<String, Object> entry : propsCopied.entrySet()) {
                matsTrace.setTraceProperty(entry.getKey(), serializer.serializeObject(entry.getValue()));
            }

            SerializedMatsTrace serializedMatsTrace = serializer.serializeMatsTrace(matsTrace);
            byte[] matsTraceBytes = serializedMatsTrace.getMatsTraceBytes();

            // Get FactoryConfig
            FactoryConfig factoryConfig = jmsMatsFactory.getFactoryConfig();

            // Create the JMS Message that will be sent.
            MapMessage mm = jmsSession.createMapMessage();
            // Set the MatsTrace.
            mm.setBytes(factoryConfig.getMatsTraceKey(), matsTraceBytes);
            mm.setString(factoryConfig.getMatsTraceKey() + MatsSerializer.META_KEY_POSTFIX,
                    serializedMatsTrace.getMeta());

            // :: Add the properties to the MapMessage
            for (Map.Entry<String, byte[]> entry : bytesCopied.entrySet()) {
                mm.setBytes(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : stringsCopied.entrySet()) {
                mm.setString(entry.getKey(), entry.getValue());
            }

            long nanosProduced = System.nanoTime();
            double millisTotal = (nanosProduced - nanosStart) / 1_000_000d;


            // :: Create the JMS Queue or Topic.
            Destination destination = toChannel.getMessagingModel() == MessagingModel.QUEUE
                    ? jmsSession.createQueue(factoryConfig.getMatsDestinationPrefix() + toChannel.getId())
                    : jmsSession.createTopic(factoryConfig.getMatsDestinationPrefix() + toChannel.getId());

            // Create JMS Producer
            // TODO : OPTIMIZE: Check how expensive this is (with the closing) - it could be cached - particularly easy
            // with the JmsSessionHolder
            MessageProducer producer = jmsSession.createProducer(destination);

            // TODO : OPTIMIZE: Use "asynchronous sends", i.e. register completion listeners (catch exceptions) and
            // close at the end.
            // NOTICE: One may createProducer with "null" as argument, instead supplying destination in send()..
            // IDEA: have asyncSend() on JmsSessionHolder, which lazy-creates a non-specific producer, registering
            // an onCompletionListener that collects a list of Exceptions upon jmsSessionHolder.closeProducer()..
            producer.send(mm);
            producer.close();

            double millisSent = (System.nanoTime() - nanosProduced) / 1_000_000d;

            // :: Pack along, and close producer.
            log.info(LOG_PREFIX + "SENDING " + what + " message to [" + destination
                    + "], MT->serialize:[" + serializedMatsTrace.getSizeUncompressed()
                    + " B, " + serializedMatsTrace.getMillisSerialization()
                    + " ms]->comp:[" + serializedMatsTrace.getMeta()
                    + " " + serializedMatsTrace.getMillisCompression()
                    + " ms]->sent:[" + matsTraceBytes.length
                    + " B] - tot w/DTO&STO:[" + millisTotal + "], send:["+millisSent+" ms]");
        }
        catch (JMSException e) {
            throw new MatsBackendException("Got problems sending [" + what + "] to [" + toChannel + "] via JMS API.", e);
        }
    }

    default String id(String what, Object obj) {
        return what + '@' + Integer.toHexString(System.identityHashCode(obj));
    }

    default String id(Object obj) {
        return obj.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(obj));
    }

    default String stageOrInit(JmsMatsTxContextKey txContextKey) {
        if (txContextKey.getStage() != null) {
            return "StageProcessor for [" + txContextKey.getStage() + "]";
        }
        return "Initiation";
    }
}
