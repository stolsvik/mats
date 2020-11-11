package com.stolsvik.mats.test.abstractunit;

import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsFactory.MatsFactoryWrapper;
import com.stolsvik.mats.MatsInitiator;
import com.stolsvik.mats.impl.jms.JmsMatsFactory;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling.PoolingKeyInitiator;
import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler_Pooling.PoolingKeyStageProcessor;
import com.stolsvik.mats.serial.MatsSerializer;
import com.stolsvik.mats.serial.MatsTrace;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * Base class containing common code for Rule_Mats and Extension_Mats located in the following modules:
 * <ul>
 *     <li>mats-test-junit</li>
 *     <li>mats-test-jupiter</li>
 * </ul>
 * This class sets up an in-vm Active MQ broker through the use of {@link MatsLocalVmActiveMq} which is again utilized
 * to create the {@link MatsFactory} which can be utilized to create unit tests which rely on testing functionality
 * utilizing MATS.
 * <p>
 * The setup and creation of these objects are located in the {@link #beforeAll()} method, this method should be called
 * through the use JUnit and Jupiters life
 *
 * @author Kevin Mc Tiernan, 2020-10-18, kmctiernan@gmail.com
 */
public abstract class AbstractMatsTest<Z> {

    protected static final Logger log = LoggerFactory.getLogger(AbstractMatsTest.class);

    protected MatsLocalVmActiveMq _matsLocalVmActiveMq;
    protected MatsSerializer<Z> _matsSerializer;
    protected MatsFactory _matsFactory;
    protected MatsInitiator _matsInitiator;
    protected CopyOnWriteArrayList<MatsFactory> _createdMatsFactories = new CopyOnWriteArrayList<>();

    /**
     * Default constructor
     */
    protected AbstractMatsTest() {
    }

    /**
     * Should one wish to utilize a non default {@link MatsSerializer}, this permits users a hook to configure this.
     *
     * @param matsSerializer
     *          to be utilized by the created {@link MatsFactory}
     */
    protected AbstractMatsTest(MatsSerializer<Z> matsSerializer) {
        _matsSerializer = matsSerializer;
    }

    /**
     * Creates an in-vm ActiveMQ Broker which again is utilized to create a {@link JmsMatsFactory}.
     * <p>
     * This method should be called as a result of the following life cycle events for either JUnit or Jupiter:
     * <ul>
     *     <li>BeforeClass - JUnit - ClassRule</li>
     *     <li>BeforeAllCallback - Jupiter</li>
     * </ul>
     */
    public void beforeAll() {
        log.debug("+++ JUnit/Jupiter +++ BEFORE_CLASS on ClassRule/Extension '" + id(getClass()) + "', JMS and MATS:");

        // ::: ActiveMQ BrokerService and ConnectionFactory
        // ==================================================

        _matsLocalVmActiveMq = MatsLocalVmActiveMq.createRandomInVmActiveMq();

        // ::: MatsFactory
        // ==================================================

        log.debug("Setting up JmsMatsFactory.");
        // Allow for override in specialization classes, in particular the one with DB.
        _matsFactory = createMatsFactory();
        log.debug("+++ JUnit/Jupiter +++ BEFORE_CLASS done on ClassRule/Extension '" + id(getClass()) + "', JMS and MATS.");
    }

    /**
     * Tear down method, stopping all {@link MatsFactory} created during a test setup and close the AMQ broker.
     * <p>
     * This method should be called as a result of the following life cycle events for either JUnit or Jupiter:
     * <ul>
     *     <li>AfterClass - JUnit - ClassRule</li>
     *     <li>AfterAllCallback - Jupiter</li>
     * </ul>
     */
    public void afterAll() {
        log.info("+++ JUnit/Jupiter +++ AFTER_CLASS on ClassRule/Extension '" + id(getClass()) + "':");
        // :: Close all MatsFactories (thereby closing all endpoints and initiators, and thus their connections).
        for (MatsFactory createdMatsFactory : _createdMatsFactories) {
            createdMatsFactory.stop(30_000);
        }

        // :: Close the AMQ Broker
        _matsLocalVmActiveMq.close();

        // :: Clear the MatsInitiator as it was killed during the Factory stop call above.
        _matsInitiator = null;

        log.info("+++ JUnit/Jupiter +++ AFTER_CLASS done on ClassRule/Extension '" + id(getClass()) + "' DONE.");
    }

    /**
     * @return the default {@link MatsInitiator} from this JUnit Rule.
     */
    public synchronized MatsInitiator getMatsInitiator() {
        if (_matsInitiator == null) {
            _matsInitiator = getMatsFactory().getDefaultInitiator();
        }
        return _matsInitiator;
    }

    /**
     * @return the JMS ConnectionFactory that this JUnit Rule sets up.
     */
    public ConnectionFactory getJmsConnectionFactory() {
        return _matsLocalVmActiveMq.getConnectionFactory();
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
    public MatsTrace<Z> getDlqMessage(String endpointId) {
        return _matsLocalVmActiveMq.getDlqMessage(_matsSerializer,
                _matsFactory.getFactoryConfig().getMatsDestinationPrefix(),
                _matsFactory.getFactoryConfig().getMatsTraceKey(),
                endpointId);
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

    /**
     * @return the {@link MatsFactory} that this JUnit Rule sets up.
     */
    public MatsFactory getMatsFactory() {
        return _matsFactory;
    }

    /**
     * Loops through all the {@link MatsFactory}'s contained within the {@link #_createdMatsFactories}, this ensures
     * that all factories are "clean".
     * <p>
     * Current usage, is to call this on the following JUnit/Jupiter life cycle events:
     * <ul>
     *     <li>After - JUnit - Rule</li>
     *     <li>AfterEachCallback - Jupiter</li>
     * </ul>
     */
    public void removeAllEndpoints() {
        // :: Loop through all created MATS factories and remove the endpoints
        for (MatsFactory createdMatsFactory : _createdMatsFactories) {
            createdMatsFactory.getEndpoints()
                    .forEach(matsEndpoint -> matsEndpoint.remove(30_000));
        }
    }

    /**
     * Should only be invoked by {@link #createMatsFactory()}.
     */
    protected MatsFactory createMatsFactory(MatsSerializer<Z> stringSerializer,
            ConnectionFactory connectionFactory) {
        JmsMatsFactory<Z> matsFactory = JmsMatsFactory.createMatsFactory_JmsOnlyTransactions(
                this.getClass().getSimpleName(), "*testing*",
                JmsMatsJmsSessionHandler_Pooling.create(connectionFactory,
                        PoolingKeyInitiator.FACTORY, PoolingKeyStageProcessor.FACTORY), // FACTORY,FACTORY is default
                stringSerializer);
        // For all test scenarios, it makes no sense to have a concurrency more than 1, unless explicitly testing that.
        matsFactory.getFactoryConfig().setConcurrency(1);
        return matsFactory;
    }

    protected String id(Class<?> clazz) {
        return clazz.getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }
}
