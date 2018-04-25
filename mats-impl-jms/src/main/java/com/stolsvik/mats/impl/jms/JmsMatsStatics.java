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

import org.slf4j.Logger;

import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsSerializer.SerializedMatsTrace;
import com.stolsvik.mats.serial.MatsTrace;

public interface JmsMatsStatics {

    String LOG_PREFIX = "#JMATS# ";

    String THREAD_PREFIX = "MATS:";

    /**
     * Common sending method - handles commonalities. <b>Notice that the bytes- and Strings-Maps come back cleared, even
     * if any part of sending throws.</b>
     */
    default <Z> void sendMatsMessage(Logger log, long nanosStart, Session jmsSession, JmsMatsFactory<Z> jmsMatsFactory,
            boolean queue, MatsTrace<Z> matsTrace, HashMap<String, Object> props,
            HashMap<String, byte[]> bytes, HashMap<String, String> strings,
            String to, String what) {
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
            Destination destination = queue
                    ? jmsSession.createQueue(factoryConfig.getMatsDestinationPrefix() + to)
                    : jmsSession.createTopic(factoryConfig.getMatsDestinationPrefix() + to);

            // Create JMS Producer
            // OPTIMIZE: Check how expensive this is (with the closing) - it could be cached.
            MessageProducer producer = jmsSession.createProducer(destination);

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
            throw new MatsBackendException("Got problems sending [" + what + "] to [" + to + "] via JMS API.", e);
        }
    }

    default String id(String what, Object obj) {
        return what + '@' + Integer.toHexString(System.identityHashCode(obj));
    }

    default String id(Object obj) {
        return obj.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(obj));
    }

    default String stageOrInit(JmsMatsStage<?, ?, ?, ?> stage) {
        if (stage != null) {
            return "Stage [" + stage.toString() + "]";
        }
        return "Initiation";
    }

    static String getHostname_internal() {
        try (BufferedInputStream in = new BufferedInputStream(Runtime.getRuntime().exec("hostname").getInputStream())) {
            byte[] b = new byte[256];
            int readBytes = in.read(b, 0, b.length);
            // Using platform default charset, which probably is exactly what we want in this one specific case.
            return new String(b, 0, readBytes).trim();
        }
        catch (Throwable t) {
            try {
                return InetAddress.getLocalHost().getHostName();
            }
            catch (UnknownHostException e) {
                return "<cannot resolve>";
            }
        }
    }

    String HOSTNAME = getHostname_internal();
}
