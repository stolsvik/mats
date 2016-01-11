package com.stolsvik.mats.amq.test;

import java.io.IOException;
import java.util.Scanner;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMqStarter {
    private static final Logger log = LoggerFactory.getLogger(ActiveMqStarter.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        ActiveMqStarter activeMQStarter;
        try (Scanner in = new Scanner(System.in)) {
            STARTLOOP: while (true) {
                activeMQStarter = new ActiveMqStarter();
                activeMQStarter.startActiveMq("tcp://localhost:61616");

                while (true) {
                    System.out.print("\n. Type 'stop' to shut down the ActiveMQ instance,"
                            + " or 'restart' shutdown'n'restart.\n$ ");
                    String nextLine = in.nextLine();
                    if ("stop".equalsIgnoreCase(nextLine)) {
                        break STARTLOOP;
                    }
                    if ("restart".equalsIgnoreCase(nextLine)) {
                        activeMQStarter.stopActiveMq();
                        System.out.println("\n.. stopped, now starting again ..\n");
                        continue STARTLOOP;
                    }
                }
            }
        }
        activeMQStarter.stopActiveMq();
    }

    private BrokerService _amqBroker;

    private void startActiveMq(String connectorUrl) {
        // ::: Server (BrokerService)
        // ====================================
        log.info("## Setting up ActiveMQ BrokerService (server).");
        _amqBroker = new BrokerService();

        // :: Set properties suitable for testing.
        _amqBroker.setBrokerName("MatsActiveMQ");
        _amqBroker.setUseJmx(false); // No need for JMX registry
        _amqBroker.setPersistent(false); // No need for persistence (prevents KahaDB dirs from being created)
        _amqBroker.setAdvisorySupport(false); // No need Advisory Messages
        // :: Add Connector
        try {
            _amqBroker.addConnector(connectorUrl);
        }
        catch (Exception e) {
            throw new IllegalStateException("Couldn't perform amqBroker.addConnector(\"" + connectorUrl + "\").", e);
        }

        // :: Add LoggerBrokerPlugin
        LoggingBrokerPlugin loggingBrokerPlugin = new LoggingBrokerPlugin();
        loggingBrokerPlugin.setLogAll(true);
        _amqBroker.setPlugins(new BrokerPlugin[] { loggingBrokerPlugin });

        // :: Set Individual DLQ
        // Hear, hear: http://activemq.2283324.n4.nabble.com/PolicyMap-api-is-really-bad-td4284307.html
        PolicyMap destinationPolicy = new PolicyMap();
        _amqBroker.setDestinationPolicy(destinationPolicy);
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setQueue(">");
        destinationPolicy.put(policyEntry.getDestination(), policyEntry);

        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("DLQ.");
        policyEntry.setDeadLetterStrategy(individualDeadLetterStrategy);

        try {
            _amqBroker.start();
        }
        catch (Exception e) {
            throw new IllegalStateException("Could not start ActiveMQ server.", e);
        }
        log.info("## ActiveMQ started!");
    }

    private void stopActiveMq() {
        log.info("## Shutting down ActiveMQ.");
        // :: Close the AMQ Broker
        try {
            _amqBroker.stop();
        }
        catch (Exception e) {
            throw new IllegalStateException("Couldn't stop AMQ Broker!", e);
        }
        log.info("## ActiveMQ shut down.");
    }
}
