package com.stolsvik.mats.test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.FactoryConfig;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.serial.MatsSerializer;

/**
 * JUnit {@link Rule} of type {@link ExternalResource} that make a convenient MATS harness, providing a
 * {@link MatsFactory} backed by an in-vm {@link BrokerService ActiveMQ instance}.
 * <p>
 * If the system property "{@link #SYSPROP_MATS_TEST_ACTIVEMQ mats.test.activemq}" is set to any string, the in-vm
 * ActiveMQ Broker instance <i>will not</i> be created, and the supplied string will be used for the
 * {@link ActiveMQConnectionFactory ActiveMQ ConnectionFactory} (i.e. the client) brokerURL. The special value
 * "{@link #SYSPROP_VALUE_LOCALHOST LOCALHOST}" implies "tcp://localhost:61616", which is the default for a localhost
 * ActiveMQ connection.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Rule_Mats extends ExternalResource {
    private static final Logger log = LoggerFactory.getLogger(Rule_Mats.class);

    /**
     * System property that if set will a) Not start in-vm ActiceMQ instance, and b) make the ConnectionFactory use the
     * value as brokerURL - with the special case that if the value is "{@link #SYSPROP_VALUE_LOCALHOST LOCAL_TCP}", it
     * will be <code>"tcp://localhost:61616"</code>.
     */
    public static final String SYSPROP_MATS_TEST_ACTIVEMQ = "mats.test.activemq";

    /**
     * If the value of {@link #SYSPROP_MATS_TEST_ACTIVEMQ} is this value, the ConnectionFactory will use
     * "tcp://localhost:61616" as the brokerURL.
     */
    public static final String SYSPROP_VALUE_LOCALHOST = "LOCALHOST";

    private BrokerService _amqServer;

    private ActiveMQConnectionFactory _amqClient;

    MatsSerializer<String> _matsSerializer;

    private MatsFactory _matsFactory;

    private static final String BROKER_NAME = "MatsLocalVmBroker";

    protected String id(Class<?> clazz) {
        return clazz.getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }

    @Override
    public void before() throws Throwable {
        log.info("+++ BEFORE on JUnit Rule '" + id(Rule_Mats.class) + "', JMS and MATS:");
        String sysprop_matsTestActiveMq = System.getProperty(SYSPROP_MATS_TEST_ACTIVEMQ);

        // ::: Server (BrokerService)
        // ====================================

        // :? Do we have specific brokerUrl to connect to?
        if (sysprop_matsTestActiveMq == null) {
            // -> No - the system property was not set, hence start the in-vm broker.
            log.info("Setting up in-vm ActiveMQ BrokerService '" + BROKER_NAME + "' (i.e. the MQ server).");
            _amqServer = new BrokerService();
            _amqServer.setBrokerName(BROKER_NAME);
            _amqServer.setUseJmx(false); // No need for JMX registry.
            _amqServer.setPersistent(false); // No need for persistence (prevents KahaDB dirs from being created).
            _amqServer.setAdvisorySupport(false); // No need Advisory Messages.
            _amqServer.setUseShutdownHook(false);

            // :: Set Individual DLQ
            // Hear, hear: http://activemq.2283324.n4.nabble.com/PolicyMap-api-is-really-bad-td4284307.html
            PolicyMap destinationPolicy = new PolicyMap();
            _amqServer.setDestinationPolicy(destinationPolicy);
            PolicyEntry policyEntry = new PolicyEntry();
            policyEntry.setQueue(">");
            destinationPolicy.put(policyEntry.getDestination(), policyEntry);

            IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
            individualDeadLetterStrategy.setQueuePrefix("DLQ.");
            policyEntry.setDeadLetterStrategy(individualDeadLetterStrategy);

            _amqServer.start();
        }
        else {
            // -> Yes, there is specified a brokerUrl to connect to, so we
            log.info("SKIPPING setup of in-vm ActiveMQ BrokerService (MQ server), since System Property '"
                    + SYSPROP_MATS_TEST_ACTIVEMQ + "' was set (to [" + sysprop_matsTestActiveMq + "]).");
        }

        // ::: Client (ConnectionFactory)
        // ====================================

        // :: Find which broker URL to use
        String brokerUrl;
        if (sysprop_matsTestActiveMq == null) {
            brokerUrl = "vm://" + BROKER_NAME + "?create=false";
        }
        else if (SYSPROP_VALUE_LOCALHOST.equals(sysprop_matsTestActiveMq)) {
            brokerUrl = "tcp://localhost:61616";
        }
        else {
            brokerUrl = sysprop_matsTestActiveMq;
        }
        // :: Connect to the broker
        log.info("Setting up ActiveMQ ConnectionFactory (MQ client), brokerUrl: [" + brokerUrl + "].");
        _amqClient = new ActiveMQConnectionFactory(brokerUrl);
        RedeliveryPolicy redeliveryPolicy = _amqClient.getRedeliveryPolicy();
        // :: Only try redelivery once, since the unit tests does not need any more to prove that they work.
        redeliveryPolicy.setInitialRedeliveryDelay(100);
        redeliveryPolicy.setUseExponentialBackOff(false);
        redeliveryPolicy.setMaximumRedeliveries(1);

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
        if (_amqServer != null) {
            log.info("AMQ Server.stop().");
            try {
                _amqServer.stop();
            }
            catch (Exception e) {
                throw new IllegalStateException("Couldn't stop AMQ Broker!", e);
            }
            log.info("AMQ Server.waitUntilStopped().");
            _amqServer.waitUntilStopped();
            // Force yield, as evidently AMQ's async shut down procedure must have some millis to fully run.
            try {
                Thread.sleep(25);
            }
            catch (InterruptedException e1) {
                throw new AssertionError("Got interrupted while sleeping - unexpected, man..!");
            }
        }
        log.info("--- AFTER done! JUnit Rule '" + id(Rule_Mats.class) + "' DONE.");
    }

    /**
     * Waits a couple of seconds for a message to appear on the Dead Letter Queue for the provided endpointId - useful
     * if the test is designed to fail a stage (i.e. that a stage raises some {@link RuntimeException}, or the special
     * {@link MatsRefuseMessageException}).
     *
     * @param endpointId the endpoint which is expected to generate a DLQ message.
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
                log.info("!! Got a DLQ Message! Length of byte serialized&compressed MatsTrace: " + matsTraceBytes.length);
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
