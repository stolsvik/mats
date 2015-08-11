package com.stolsvik.mats.impl.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;

import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.MatsTrace;
import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.util.MatsStringSerializer;
import com.stolsvik.mats.util.MatsStringSerializer;

public interface JmsMatsStatics {

    String LOG_PREFIX = "#JMATS# ";

    String THREAD_PREFIX = "MATS:";

    int DEFAULT_DELAY_MILLIS = 50;

    default void sendMessage(Logger log, Session jmsSession, FactoryConfig factoryConfig,
            MatsStringSerializer matsStringSerializer, boolean queue,
            MatsTrace matsTrace, String to, String what) {
        try {
            MapMessage mm = jmsSession.createMapMessage();
            mm.setString(factoryConfig.getMatsTraceKey(), matsStringSerializer.serializeMatsTrace(matsTrace));

            Destination destination = queue
                    ? jmsSession.createQueue(factoryConfig.getMatsDestinationPrefix() + to)
                    : jmsSession.createTopic(factoryConfig.getMatsDestinationPrefix() + to);

            MessageProducer producer = jmsSession.createProducer(destination);

            log.info(LOG_PREFIX + "SENDING " + what + " message to [" + destination + "].");

            producer.send(mm);
            producer.close();
        }
        catch (JMSException e) {
            throw new MatsBackendException("Got problems sending [" + what + "] to [" + to + "] via JMS API.", e);
        }
    }
}
