package io.mats3.amq;

import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatsLoggingBrokerPlugin extends BrokerFilter implements BrokerPlugin {

    private static String LOG_PREFIX = "#MATS# ";

    private static final Logger log = LoggerFactory.getLogger(MatsLoggingBrokerPlugin.class);

    public MatsLoggingBrokerPlugin() {
        super(null);
    }

    private MatsLoggingBrokerPlugin(Broker next) {
        super(next);
    }

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        return new MatsLoggingBrokerPlugin(broker);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        log("SEND", messageSend);
        next.send(producerExchange, messageSend);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        log.debug(LOG_PREFIX + "\nPOST PROCESS DISPATCH"
                + "\n  DeliverySequenceId:" + messageDispatch.getDeliverySequenceId()
                + "\n  RedeliveryCounter:" + messageDispatch.getRedeliveryCounter()
                + "\n  CommandId:" + messageDispatch.getCommandId()
                + "\n  DataStructureType:" + messageDispatch.getDataStructureType());
        log(" Message", messageDispatch.getMessage());
        next.postProcessDispatch(messageDispatch);
    }

    private void log(String what, Message messageSend) {
        String mats = "";
        if (messageSend instanceof ActiveMQMapMessage) {
            ActiveMQMapMessage matsMm = (ActiveMQMapMessage) messageSend;
            String matsTrace;
            try {
                matsTrace = matsMm.getString("mats:trace");// TODO: Use static.
                if (matsTrace != null) {
                    mats = "\n\n MATS:"
                            + "\n  MatsTrace:         " + matsTrace;
                }
            }
            catch (JMSException e) {
                throw new AssertionError("Getting a String from ActiveMqMapMessage threw!", e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(LOG_PREFIX + '\n' + what + ": " + messageSend.getClass().getSimpleName()
                    + '@' + Integer.toHexString(System.identityHashCode(messageSend))
                    + "\n  currentTimeMillis: " + System.currentTimeMillis()
                    + "\n  Timestamp:         " + messageSend.getTimestamp()
                    + "\n  BrokerInTime:      " + messageSend.getBrokerInTime()
                    + "\n  BrokerOutTime:     " + messageSend.getBrokerInTime()
                    + "\n  MessageId:         " + messageSend.getMessageId()
                    + "\n  ProducerId:        " + messageSend.getProducerId()
                    + "\n  TransactionId:     " + messageSend.getTransactionId()
                    + "\n  OrigTransactionId: " + messageSend.getOriginalTransactionId()
                    + "\n  Destination:       " + messageSend.getDestination()
                    + "\n  OrigDestination:   " + messageSend.getOriginalDestination()
                    + "\n  Size:              " + messageSend.getSize()
                    + "\n  RedeliveryCounter: " + messageSend.getRedeliveryCounter()
                    + mats);
        }
    }

}
