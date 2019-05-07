package com.stolsvik.mats.test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;

/**
 * JUnit {@link Rule} of type {@link ExternalResource} that make a convenient MATS harness, providing a
 * {@link MatsFactory} backed by an in-vm {@link BrokerService ActiveMQ instance}.
 * <p>
 * Please read JavaDoc of {@link MatsTestActiveMq} to see what system properties are available to control the broker
 * creation.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Rule_Mats extends ExternalResource {
    private static final Logger log = LoggerFactory.getLogger(Rule_Mats.class);


    private BrokerService _amqServer;

    private ActiveMQConnectionFactory _amqClient;

    MatsSerializer<String> _matsSerializer;

    private MatsFactory _matsFactory;

    protected String id(Class<?> clazz) {
        return clazz.getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }

    @Override
    public void before() throws Throwable {
        log.info("+++ BEFORE on JUnit Rule '" + id(Rule_Mats.class) + "', JMS and MATS:");

        // ::: Server (BrokerService)
        // ====================================

        _amqServer = MatsTestActiveMq.createBrokerService();

        // ::: Client (ConnectionFactory)
        // ====================================

        _amqClient = MatsTestActiveMq.createConnectionFactory();

        // ::: MatsFactory
        // ====================================

        log.info("Setting up JmsMatsFactory.");
        _matsSerializer = new MatsSerializer_DefaultJson();
        // Allow for override in specialization classes, in particular the one with DB.
        _matsFactory = createMatsFactory(_matsSerializer, _amqClient);
        // For all test scenarios, it makes no sense to have a concurrency more than 1, unless explicitly testing that.
        _matsFactory.getFactoryConfig().setConcurrency(1);
        log.info("--- BEFORE done! JUnit Rule '" + id(Rule_Mats.class) + "', JMS and MATS.");
    }

    protected MatsFactory createMatsFactory(MatsSerializer<String> stringSerializer,
            ConnectionFactory connectionFactory) {
        return JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(this.getClass().getSimpleName(),
                "*testing*",
                new JmsMatsJmsSessionHandler_Pooling((s) -> connectionFactory.createConnection()),
                _matsSerializer);
    }

    @Override
    public void after() {
        log.info("+++ AFTER on JUnit Rule '" + id(Rule_Mats.class) + "':");
        // :: Close the MatsFactory (thereby closing all endpoints and initiators, and thus their connections).
        _matsFactory.stop();

        // :: Close the AMQ Broker
        MatsTestActiveMq.stopBrokerService(_amqServer);

        log.info("--- AFTER done! JUnit Rule '" + id(Rule_Mats.class) + "' DONE.");
    }

    /**
     * Waits a couple of seconds for a message to appear on the Dead Letter Queue for the provided endpointId - useful
     * if the test is designed to fail a stage (i.e. that a stage raises some {@link RuntimeException}, or the special
     * {@link MatsRefuseMessageException}).
     *
     * @param endpointId
     *            the endpoint which is expected to generate a DLQ message.
     * @return the {@link MatsTrace} of the DLQ'ed message.
     */
    public MatsTrace<String> getDlqMessage(String endpointId) {
        FactoryConfig factoryConfig = getMatsFactory().getFactoryConfig();
        String dlqQueueName = "DLQ." + factoryConfig.getMatsDestinationPrefix() + endpointId;
        try {
            Connection jmsConnection = _amqClient.createConnection();
            try {
                Session jmsSession = jmsConnection.createSession(true, Session.SESSION_TRANSACTED);
                Queue dlqQueue = jmsSession.createQueue(dlqQueueName);
                MessageConsumer dlqConsumer = jmsSession.createConsumer(dlqQueue);
                jmsConnection.start();

                final int maxWaitMillis = 5000;
                log.info("Listening for message on queue [" + dlqQueueName + "].");
                Message msg = dlqConsumer.receive(maxWaitMillis);

                if (msg == null) {
                    throw new AssertionError("Did not get a message on the queue [" + dlqQueueName + "] within "
                            + maxWaitMillis + "ms.");
                }

                MapMessage matsMM = (MapMessage) msg;
                byte[] matsTraceBytes = matsMM.getBytes(factoryConfig.getMatsTraceKey());
                log.info("!! Got a DLQ Message! Length of byte serialized&compressed MatsTrace: "
                        + matsTraceBytes.length);
                jmsSession.commit();
                jmsConnection.close(); // Closes session and consumer
                return _matsSerializer.deserializeMatsTrace(matsTraceBytes,
                        matsMM.getString(factoryConfig.getMatsTraceKey() + ":meta")).getMatsTrace();
            }
            finally {
                jmsConnection.close();
            }
        }
        catch (JMSException e) {
            throw new IllegalStateException("Got a JMSException when trying to receive Mats message on [" + dlqQueueName
                    + "].", e);
        }
    }

    /**
     * @return the {@link MatsFactory} that this JUnit Rule sets up.
     */
    public MatsFactory getMatsFactory() {
        return _matsFactory;
    }

    private MatsInitiator _matsInitiator;

    /**
     * @return the default {@link MatsInitiator} from this JUnit Rule.
     */
    public synchronized MatsInitiator getMatsInitiator() {
        if (_matsInitiator == null) {
            _matsInitiator = getMatsFactory().createInitiator();
        }
        return _matsInitiator;
    }
}
