package com.stolsvik.mats.test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.rules.ExternalResource;

import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.util.MatsDefaultJsonSerializer;

public class Rule_Mats extends ExternalResource {

    private BrokerService _amqServer;

    private ActiveMQConnectionFactory _amqClient;

    private MatsFactory _matsFactory;

    @Override
    protected void before() throws Throwable {
        // ::: Server (BrokerService)
        // ====================================
        _amqServer = new BrokerService();
        _amqServer.setBrokerName("localhost");
        _amqServer.setUseJmx(false); // No need for JMX registry
        _amqServer.setPersistent(false); // No need for persistence (prevents KahaDB dirs from being created)
        _amqServer.setAdvisorySupport(false); // No need Advisory Messages

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

        // ::: Client (ConnectionFactory)
        // ====================================
        _amqClient = new ActiveMQConnectionFactory("vm://localhost?create=false");
        RedeliveryPolicy redeliveryPolicy = _amqClient.getRedeliveryPolicy();
        // :: Only try redelivery once, since the unit tests does not need any more to prove that they work.
        redeliveryPolicy.setInitialRedeliveryDelay(500);
        redeliveryPolicy.setUseExponentialBackOff(false);
        redeliveryPolicy.setMaximumRedeliveries(1);

        // ::: MatsFactory
        // ====================================
        _matsFactory = JmsMatsFactory.createMatsFactory(_amqClient, new MatsDefaultJsonSerializer());
    }

    @Override
    protected void after() {
        // :: Close the MatsFactory (thereby closing all endpoints and initiators, and thus their connections).
        _matsFactory.close();

        // :: Close the AMQ Broker
        try {
            _amqServer.stop();
        }
        catch (Exception e) {
            throw new AssertionError("Couldn't stop AMQ Broker!", e);
        }
    }

    public MatsFactory getMatsFactory() {
        return _matsFactory;
    }
}
