package com.stolsvik.mats.amq.test;

import java.io.IOException;
import java.util.Scanner;

import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.util.LoggingBrokerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.amq.MatsLoggingBrokerPlugin;

public class ActiveMqStarter {
    private static final Logger log = LoggerFactory.getLogger(ActiveMqStarter.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        BrokerService brokerService;
        try (Scanner in = new Scanner(System.in)) {
            STARTLOOP: while (true) {
                brokerService = ActiveMqStarter.startActiveMq("tcp://localhost:61616");

                while (true) {
                    System.out.print("\n. Type 'stop' to shut down the ActiveMQ instance,"
                            + " or 'restart' to shutdown'n'restart.\n$ ");
                    String nextLine = in.nextLine();
                    if ("stop".equalsIgnoreCase(nextLine)) {
                        break STARTLOOP;
                    }
                    if ("restart".equalsIgnoreCase(nextLine)) {
                        ActiveMqStarter.stopActiveMq(brokerService);
                        System.out.println("\n.. stopped, now starting again ..\n");
                        continue STARTLOOP;
                    }
                }
            }
        }
        ActiveMqStarter.stopActiveMq(brokerService);
    }

    /**
     * @param connectorUrl
     */
    private static BrokerService startActiveMq(String connectorUrl) {
        // ::: Server (BrokerService)
        // ====================================
        log.info("## Setting up ActiveMQ BrokerService (server).");
        BrokerService brokerService = new BrokerService();

        // :: Set properties suitable for testing.
        brokerService.setBrokerName("MatsActiveMQ");
        brokerService.setUseJmx(false); // No need for JMX registry
        brokerService.setPersistent(false); // No need for persistence (prevents KahaDB dirs from being created)
        brokerService.setAdvisorySupport(false); // No need Advisory Messages
        // :: Add Connector
        try {
            brokerService.addConnector(connectorUrl);
        }
        catch (Exception e) {
            throw new IllegalStateException("Couldn't perform amqBroker.addConnector(\"" + connectorUrl + "\").", e);
        }

        // :: Add LoggerBrokerPlugin
        LoggingBrokerPlugin loggingBrokerPlugin = new LoggingBrokerPlugin();
        loggingBrokerPlugin.setLogAll(true);
        brokerService.setPlugins(new BrokerPlugin[] { loggingBrokerPlugin, new MatsLoggingBrokerPlugin() });

        // :: Set Individual DLQ
        // Hear, hear: http://activemq.2283324.n4.nabble.com/PolicyMap-api-is-really-bad-td4284307.html
        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("DLQ.");

        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setQueue(">");
        policyEntry.setDeadLetterStrategy(individualDeadLetterStrategy);

        PolicyMap destinationPolicy = new PolicyMap();
        destinationPolicy.put(policyEntry.getDestination(), policyEntry);

        brokerService.setDestinationPolicy(destinationPolicy);

        try {
            brokerService.start();
        }
        catch (Exception e) {
            throw new IllegalStateException("Could not start ActiveMQ server.", e);
        }

        Broker broker;
        try {
            broker = brokerService.getBroker();
        }
        catch (Exception e) {
            throw new IllegalStateException("Couldn't ask for BrokerService.getBroker().", e);
        }
        log.info("## ActiveMQ started!  [AMQ version: " + ActiveMQConnectionMetaData.PROVIDER_VERSION
                + ", name: " + brokerService.getBrokerName()
                + " - " + broker.getBrokerId() + "]");
        return brokerService;
    }

    private static void stopActiveMq(BrokerService brokerService) {
        log.info("## Shutting down ActiveMQ.");
        // :: Close the AMQ Broker
        try {
            brokerService.stop();
        }
        catch (Exception e) {
            throw new IllegalStateException("Couldn't stop AMQ BrokerService!", e);
        }
        log.info("## ActiveMQ shut down.");
    }
}
