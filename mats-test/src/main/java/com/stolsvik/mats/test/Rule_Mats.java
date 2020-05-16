package com.stolsvik.mats.test;

import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.ConnectionFactory;

import org.apache.activemq.broker.BrokerService;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.serial.json.MatsSerializer_DefaultJson;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * JUnit {@link Rule} of type {@link ExternalResource} that make a convenient MATS harness, providing a
 * {@link MatsFactory} backed by an in-vm {@link BrokerService ActiveMQ instance}.
 * <p>
 * Please read JavaDoc of {@link MatsLocalVmActiveMq} to see what system properties are available to control the broker
 * creation.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class Rule_Mats extends ExternalResource {
    private static final Logger log = LoggerFactory.getLogger(Rule_Mats.class);

    private MatsLocalVmActiveMq _matsLocalVmActiveMq;

    MatsSerializer<String> _matsSerializer;

    private MatsFactory _matsFactory;

    protected String id(Class<?> clazz) {
        return clazz.getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }

    @Override
    public void before() {
        log.info("+++ BEFORE on JUnit Rule '" + id(Rule_Mats.class) + "', JMS and MATS:");

        // ::: ActiveMQ BrokerService and ConnectionFactory
        // ==================================================

        _matsLocalVmActiveMq = MatsLocalVmActiveMq.createRandomInVmActiveMq();

        // ::: MatsFactory
        // ==================================================

        log.info("Setting up JmsMatsFactory.");
        _matsSerializer = new MatsSerializer_DefaultJson();
        // Allow for override in specialization classes, in particular the one with DB.
        _matsFactory = createMatsFactory();
        log.info("--- BEFORE done! JUnit Rule '" + id(Rule_Mats.class) + "', JMS and MATS.");
    }

    private CopyOnWriteArrayList<MatsFactory> _createdMatsFactories = new CopyOnWriteArrayList<>();

    /**
     * Should only be invoked by {@link #createMatsFactory()}.
     */
    protected MatsFactory createMatsFactory(MatsSerializer<String> stringSerializer,
            ConnectionFactory connectionFactory) {
        JmsMatsFactory<String> matsFactory = JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(
                this.getClass().getSimpleName(), "*testing*",
                JmsMatsJmsSessionHandler_Pooling.create(connectionFactory),
                _matsSerializer);
        // For all test scenarios, it makes no sense to have a concurrency more than 1, unless explicitly testing that.
        matsFactory.getFactoryConfig().setConcurrency(1);
        return matsFactory;
    }

    /**
     * This method is public for a single reason: If you need a <i>new, separate</i> {@link MatsFactory} using the same
     * JMS ConnectionFactory as the one provided by {@link #getMatsFactory()}. The only currently known reason for this
     * is if you want to register two endpoints with the same endpointId, and the only reason for this again is to test
     * {@link MatsFactory#subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)
     * subscriptionTerminators}.
     *
     * @return a <i>new, separate</i> {@link MatsFactory} in addition to the one provided by {@link #getMatsFactory()}.
     */
    public MatsFactory createMatsFactory() {
        MatsFactory matsFactory = createMatsFactory(_matsSerializer, _matsLocalVmActiveMq.getConnectionFactory());
        // Add it to the list of created MatsFactories.
        _createdMatsFactories.add(matsFactory);
        return matsFactory;
    }

    @Override
    public void after() {
        log.info("+++ AFTER on JUnit Rule '" + id(Rule_Mats.class) + "':");
        // :: Close all MatsFactories (thereby closing all endpoints and initiators, and thus their connections).
        for (MatsFactory createdMatsFactory : _createdMatsFactories) {
            createdMatsFactory.stop(30_000);
        }

        // :: Close the AMQ Broker
        _matsLocalVmActiveMq.close();

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
        return _matsLocalVmActiveMq.getDlqMessage(_matsSerializer,
                _matsFactory.getFactoryConfig().getMatsDestinationPrefix(),
                _matsFactory.getFactoryConfig().getMatsTraceKey(),
                endpointId);
    }

    /**
     * @return the {@link MatsFactory} that this JUnit Rule sets up.
     */
    public MatsFactory getMatsFactory() {
        return _matsFactory;
    }

    /**
     * @return the JMS ConnectionFactory that this JUnit Rule sets up.
     */
    public ConnectionFactory getJmsConnectionFactory() {
        return _matsLocalVmActiveMq.getConnectionFactory();
    }

    private MatsInitiator _matsInitiator;

    /**
     * @return the default {@link MatsInitiator} from this JUnit Rule.
     */
    public synchronized MatsInitiator getMatsInitiator() {
        if (_matsInitiator == null) {
            _matsInitiator = getMatsFactory().getDefaultInitiator();
        }
        return _matsInitiator;
    }
}
