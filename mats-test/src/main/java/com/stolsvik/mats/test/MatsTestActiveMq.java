package com.stolsvik.mats.test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If the system property "{@link #SYSPROP_MATS_TEST_ACTIVEMQ mats.test.activemq}" is set to any string, the in-vm
 * ActiveMQ Broker instance <i>will not</i> be created, and the supplied string will be used for the
 * {@link ActiveMQConnectionFactory ActiveMQ ConnectionFactory} (i.e. the client) brokerURL. The special value
 * "{@link #SYSPROP_VALUE_LOCALHOST LOCALHOST}" implies "tcp://localhost:61616", which is the default for a localhost
 * ActiveMQ connection.
 * 
 * @author Endre Stølsvik 2019-05-06 22:42 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsTestActiveMq {

    /**
     * System property ("-D" jvm argument) that if set will a) Not start in-vm ActiceMQ instance, and b) make the
     * ConnectionFactory use the value as brokerURL - with the special case that if the value is
     * "{@link #SYSPROP_VALUE_LOCALHOST LOCALHOST}", it will be <code>"tcp://localhost:61616"</code>.
     * <p>
     * Value is {@code "mats.test.activemq"}
     */
    public static final String SYSPROP_MATS_TEST_ACTIVEMQ = "mats.test.activemq";

    /**
     * If the value of {@link #SYSPROP_MATS_TEST_ACTIVEMQ} is this value, the ConnectionFactory will use
     * "tcp://localhost:61616" as the brokerURL.
     * <p>
     * Value is {@code "LOCALHOST"}
     */
    public static final String SYSPROP_VALUE_LOCALHOST = "LOCALHOST";

    /**
     * Name of the local broker.
     * <p>
     * Value is {@code "MatsLocalVmBroker"}
     */
    private static final String BROKER_NAME = "MatsLocalVmBroker";

    private static final Logger log = LoggerFactory.getLogger(MatsTestActiveMq.class);

    /**
     * <b>Remember to {@link #stopBrokerService(BrokerService) stop the broker} after use!</b>
     * 
     * @return a newly created ActiveMQ BrokerService, <b>unless</b> the {@link #SYSPROP_MATS_TEST_ACTIVEMQ} is set to
     *         something, <b>in which case it returns <code>null</code></b> (The broker is then assumed to be running
     *         outside of this JVM).
     */
    public static BrokerService createBrokerService() {
        String sysprop_matsTestActiveMq = System.getProperty(SYSPROP_MATS_TEST_ACTIVEMQ);

        // :? Do we have specific brokerUrl to connect to?
        if (sysprop_matsTestActiveMq == null) {
            // -> No - the system property was not set, hence start the in-vm broker.
            log.info("Setting up in-vm ActiveMQ BrokerService '" + BROKER_NAME + "' (i.e. the MQ server).");
            BrokerService _amqBrokerService = new BrokerService();
            _amqBrokerService.setBrokerName(BROKER_NAME);
            _amqBrokerService.setUseJmx(false); // No need for JMX registry.
            _amqBrokerService.setPersistent(false); // No need for persistence (prevents KahaDB dirs from being
                                                    // created).
            _amqBrokerService.setAdvisorySupport(false); // No need Advisory Messages.
            _amqBrokerService.setUseShutdownHook(false);

            // :: Set Individual DLQ
            // Hear, hear: http://activemq.2283324.n4.nabble.com/PolicyMap-api-is-really-bad-td4284307.html
            PolicyMap destinationPolicy = new PolicyMap();
            _amqBrokerService.setDestinationPolicy(destinationPolicy);
            PolicyEntry policyEntry = new PolicyEntry();
            policyEntry.setQueue(">");
            destinationPolicy.put(policyEntry.getDestination(), policyEntry);

            IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
            individualDeadLetterStrategy.setQueuePrefix("DLQ.");
            policyEntry.setDeadLetterStrategy(individualDeadLetterStrategy);

            try {
                _amqBrokerService.start();
            }
            catch (Exception e) {
                throw new AssertionError("Could not start ActiveMQ BrokerService.", e);
            }
            return _amqBrokerService;
        }
        // E-> Yes, there is specified a brokerUrl to connect to, so we don't start in-vm ActiveMQ.
        log.info("SKIPPING setup of in-vm ActiveMQ BrokerService (MQ server), since System Property '"
                + SYSPROP_MATS_TEST_ACTIVEMQ + "' was set (to [" + sysprop_matsTestActiveMq + "]).");
        return null;
    }

    /**
     * @return an ActiveMq JMS ConnectionFactory, based on the value of system property
     *         {@link #SYSPROP_MATS_TEST_ACTIVEMQ} - if this is not set, it connects to the BrokerService that was
     *         created with {@link #createBrokerService()}.
     */
    public static ActiveMQConnectionFactory createConnectionFactory() {
        String sysprop_matsTestActiveMq = System.getProperty(SYSPROP_MATS_TEST_ACTIVEMQ);

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
        ActiveMQConnectionFactory amqClient = new ActiveMQConnectionFactory(brokerUrl);
        RedeliveryPolicy redeliveryPolicy = amqClient.getRedeliveryPolicy();
        // :: Only try redelivery once, since the unit tests does not need any more to prove that they work.
        redeliveryPolicy.setInitialRedeliveryDelay(100);
        redeliveryPolicy.setUseExponentialBackOff(false);
        redeliveryPolicy.setMaximumRedeliveries(1);

        return amqClient;
    }

    /**
     * Stops the supplied BrokerService <b>if non-null</b>
     * 
     * @param brokerService
     *            the BrokerService that was gotten with {@link #createBrokerService()}, which might be null.
     */
    public static void stopBrokerService(BrokerService brokerService) {
        if (brokerService != null) {
            log.info("AMQ Server.stop().");
            try {
                brokerService.stop();
            }
            catch (Exception e) {
                throw new IllegalStateException("Couldn't stop AMQ Broker!", e);
            }
            log.info("AMQ Server.waitUntilStopped().");
            brokerService.waitUntilStopped();
            // Force yield, as evidently AMQ's async shut down procedure must have some millis to fully run.
            try {
                Thread.sleep(25);
            }
            catch (InterruptedException e1) {
                throw new AssertionError("Got interrupted while sleeping - unexpected, man..!");
            }
        }
    }

}
