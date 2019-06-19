package com.stolsvik.mats.spring.jms.factories;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.stolsvik.mats.spring.jms.factories.ConnectionFactoryScenarioWrapper.ConnectionFactoryProvider;
import com.stolsvik.mats.spring.jms.factories.ConnectionFactoryScenarioWrapper.MatsScenario;
import com.stolsvik.mats.spring.jms.factories.ConnectionFactoryScenarioWrapper.ScenarioDecider;
import com.stolsvik.mats.util_activemq.MatsLocalVmActiveMq;

/**
 * Provides a factory for a Spring-integrated Wrapper/Facade around a JMS ConnectionFactory, which in addition to
 * supporting the production setup, also facilitates the development situation where you often want to run against
 * either an in-vm MQ Broker or against a MQ Broker running on localhost, and also integrates with the
 * "mats-spring-test" integration test library where when run with the <code>MatsTestProfile</code> you will most
 * probably want an in-vm setup (typically mocking up the project-external Mats endpoints that the tested endpoints
 * collaborate with). In both the in-vm broker situations, the JMS ConnectionFactory instantiation lambda you provide
 * for what will be used in the production setup will <b>not</b> be invoked, but instead an in-vm ActiveMQ Broker will
 * be started, and an ActiveMQ ConnectionFactory will be produced.
 * <p>
 * Note: The actual decision logic and "Spring interfacing" is done in {@link ConnectionFactoryScenarioWrapper}, which
 * is the actual class of instances that will end up as Spring beans - this class is a producer of such instances,
 * simplifying the configuration, providing sane defaults.
 * <p>
 * Do note that this is purely a convenience functionality, and is in no way required for the Mats system to work.
 * <p>
 * As a matter of fact, you can achieve the exact same with Spring's Profiles (or any other "switch configuration based
 * on some property or state"-logic you can cook up), but this class effectively represents an opinionated
 * implementation of the config switching that will be needed in most scenarios working with Mats, and will probably
 * yield less configuration overall. Moreover, Mats will most probably be employed in a micro/multiple service
 * architecture, and in such situations, it is nice to make some in-house library that constitute basic building blocks
 * that all the services will need. With that in mind, this class also facilitates so you can easily construct an
 * <code>@CompanyMatsSetup</code>-type annotation which alone will pull up both the needed ConnectionFactory (with URL
 * and whatever needed already set) and MatsFactory, which handles all the mentioned scenarios, by merely placing that
 * annotation on the main config class of your service.
 * <p>
 * The point is this: If you are new to Message Queue based development, you will probably soon sit with the problem of
 * how to create a code base that efficiently lets you run it in production, while also is easy to develop on, and being
 * simple to perform integration test on your endpoints - the problem being that you need different types of
 * ConnectionFactories for the different scenarios, and in some cases also needing an in-vm MQ Broker.
 * <p>
 * There are typically four different scenarios that you need to juggle between, the two latter being identical wrt. the
 * Mats setup (which is the reason that {@link MatsScenario} only has three scenarios defined):
 *
 * <ul>
 * <li><b>Production</b> (and other production-like environments, e.g. Staging/Pre-prod): Connect to the common
 * production (or staging) MQ Broker that all other services and their Mats endpoints connect to.</li>
 * 
 * <li><b>Localhost development</b>, where you are developing on one project employing Mats Endpoints, but also run this
 * project's required collaborating projects/services with their Mats Endpoints on your local development box (typically
 * running the project you currently develop on directly in the IDE, while the other can either run in IDE or on command
 * line): Connect to a MQ Broker that you've fired up on your local development box, i.e. localhost, which the other
 * services running locally also connects to.</li>
 * 
 * <li><b>Local-VM development</b>, where you just want to run the service alone, probably with some of the other
 * collaborating Mats Endpoints from other projects/services mocked up in the same project: Connect to an in-vm MQ
 * Broker. <b>An important thing to ensure here is that these mocks, which by definition resides on the same classpath,
 * must <i>NOT</i> end up running in the production environment!</b></li>
 * 
 * <li><b>Local-VM integration testing</b>, where you have tests either utilizing the full app config with overwriting
 * of certain beans/endpoints using "Remock" (or @MockBean) and mocking out collaborating Mats Endpoints from other
 * projects/services, or piece together the required beans/endpoints using e.g. @Import and mocking out other endpoints:
 * Connect to an in-vm MQ Broker.</li>
 * </ul>
 * 
 * Please now read (all the) JavaDoc of {@link MatsProfiles} and its constants!
 * <p>
 * For the localhost and localVM scenarios, this class by default utilizes the ActiveMQ Broker and ConnectionFactory.
 * For all scenarios, but in particular for the "regular" (production-like), you can provide whatever ConnectionFactory
 * you want (configured as you desire) - this class will just wrap it.
 * 
 * @author Endre Stølsvik 2019-06-08 00:26 - http://stolsvik.com/, endre@stolsvik.com
 */
public class JmsSpringConnectionFactoryProducer implements MatsProfiles {

    /**
     * Required: The ConnectionFactoryProvider provided here is the one that will be used in the "regular" situations
     * like Production and Production-like environments, e.g. Staging, Acceptance Testing, Pre-Prod or whatever you call
     * those environments.
     * <p>
     * Notice that deciding between Production and e.g. Staging and which corresponding ConnectionFactory URL or even
     * type of ConnectionFactory you should employ here, is up to you: You might be using configuration properties,
     * Spring's Profile concept, System Properties (java cmd line "-D"-properties), or system environment variables -
     * only you know!
     * <p>
     * Notice: You are required to set up this provider lambda. However, if you are just setting up your project,
     * wanting to start writing tests, or otherwise only use the {@link MatsScenario#LOCALVM LocalVM scenario}, and thus
     * will not utilize the "regular" scenario just yet, you could just supply a lambda here that throws e.g. an
     * IllegalStateException or somesuch - which probably will remind you to fix this before going into production!
     * 
     * @param providerLambda
     *            a provider for the ConnectionFactory to use in the "regular" scenario.
     * @return <code>this</code>, for chaining.
     */
    public JmsSpringConnectionFactoryProducer regularConnectionFactory(ConnectionFactoryProvider providerLambda) {
        _regularConnectionFactoryProvider = providerLambda;
        return this;
    }

    /**
     * Optional: Provide a ConnectionFactoryProvider lambda for the "localhost" scenario. If this is not provided, it
     * will be a somewhat specially tailored ActiveMQ ConnectionFactory with the URL
     * <code>"tcp://localhost:61616?threadName"</code>, read more in the
     * <a href="https://activemq.apache.org/tcp-transport-reference">ActiveMQ documentation</a>. The tailoring entails
     * DLQ after 1 retry, and 100 ms delay between delivery and the sole redelivery attempt.
     *
     * @param providerLambda
     *            a provider for the ConnectionFactory to use in the "localhost development" scenario.
     * @return <code>this</code>, for chaining.
     * @see <a href="https://activemq.apache.org/tcp-transport-reference">ActiveMQ TCP Transport Reference</a>
     */
    public JmsSpringConnectionFactoryProducer localhostConnectionFactory(
            ConnectionFactoryProvider providerLambda) {
        _localhostConnectionFactoryProvider = providerLambda;
        return this;
    }

    /**
     * Very optional: Provide a ConnectionFactoryProvider lambda for the "local vm" scenario. You are expected to fire
     * up an in-vm MQ Broker and return a ConnectionFactory that connects to this in-vm MQ Broker. You probably want to
     * have this ConnectionFactory wrapped in your own extension of {@link ConnectionFactoryWithStartStopWrapper}, which
     * provide a way to start and stop the in-vm MQ Broker. If this configuration is not provided (and not having to
     * handle this mess is a big point of this class, so do not provide this unless you want a different broker
     * entirely!), the class {@link MatsLocalVmActiveMq} will be instantiated by invoking
     * {@link MatsLocalVmActiveMq#createInVmActiveMq(String)}, where the 'brokerName' will be the bean-name of the
     * resulting Spring Bean created by this class - and from this, the
     * {@link MatsLocalVmActiveMq#getConnectionFactory() ConnectionFactory will be gotten}.
     * <p>
     * Notice that the {@link MatsLocalVmActiveMq} has a small extra feature, where you can have it produce a
     * ConnectionFactory to localhost or a specific URL instead of the in-vm MQ Broker it otherwise produces, by means
     * of specifying a special system property ("-D" options) (In this case, it does not create the in-vm MQ Broker
     * either). The rationale for this extra feature is if you wonder how big a difference it makes to run your tests
     * against a proper MQ Broker instead of the in-vm and fully non-persistent MQ Broker you by default get. Read
     * JavaDoc at {@link MatsLocalVmActiveMq}.
     *
     * @param providerLambda
     *            a provider for the ConnectionFactory to use in the "in-vm-local development" scenario.
     * @return <code>this</code>, for chaining.
     * @see MatsLocalVmActiveMq
     */
    public JmsSpringConnectionFactoryProducer localVmConnectionFactory(ConnectionFactoryProvider providerLambda) {
        _localVmConnectionFactoryProvider = providerLambda;
        return this;
    }

    /**
     * Can optionally be overridden should you decide to use a different decision-scheme than the
     * {@link ConfigurableScenarioDecider#getDefaultScenarioDecider() default}. We need a decision for which
     * MatsScenario is in effect for this JVM, and the default ScenarioDecider can be overridden here. <i>(Do note that
     * if you want to run integration tests against a specific Active MQ Broker, there is already a feature for that in
     * {@link MatsLocalVmActiveMq}, as mentioned in the JavaDoc of
     * {@link #localVmConnectionFactory(ConnectionFactoryProvider) localVmConnectionFactory(..)}).</i>
     * <p>
     * How you otherwise decide between {@link MatsScenario#REGULAR REGULAR}, {@link MatsScenario#LOCALHOST LOCALHOST}
     * and {@link MatsScenario#LOCALVM LOCALVM} is up to you. <i>(For REGULAR, you will again have to decide whether you
     * are in Production or Staging or whatever, if this e.g. gives different URLs or changes which ConnectionFactory
     * class to instantiate - read more at {@link #regularConnectionFactory(ConnectionFactoryProvider)
     * regularConnectionFactory(..)}).</i>
     * 
     * @param scenarioDecider
     *            the {@link ScenarioDecider} that should decide between the three {@link MatsScenario}s.
     * @return <code>this</code>, for chaining.
     */
    public JmsSpringConnectionFactoryProducer scenarioDecider(ScenarioDecider scenarioDecider) {
        _scenarioDecider = scenarioDecider;
        return this;
    }

    /**
     * Creates the appropriate {@link ConnectionFactory}, which is a wrapper integrating with Spring - the decision
     * between the configured scenarios is done after all Spring beans are defined.
     * 
     * @return the appropriate {@link ConnectionFactory}, which is a wrapper integrating with Spring.
     */
    public ConnectionFactoryScenarioWrapper create() {
        if (_regularConnectionFactoryProvider == null) {
            throw new IllegalStateException("You have not provided the regular ConnectionFactory lambda.");
        }
        return new ConnectionFactoryScenarioWrapper(_regularConnectionFactoryProvider,
                _localhostConnectionFactoryProvider,
                _localVmConnectionFactoryProvider,
                _scenarioDecider);
    }

    // ====== Implementation

    private ConnectionFactoryProvider _regularConnectionFactoryProvider;
    private ConnectionFactoryProvider _localhostConnectionFactoryProvider;
    private ConnectionFactoryProvider _localVmConnectionFactoryProvider;
    private ScenarioDecider _scenarioDecider;

    {
        // :: The "mats-localhost" variant directly returns a ActiveMQConnectionFactory to standard localhost.
        localhostConnectionFactory((springEnvironment) -> new ActiveMQConnectionFactory(
                "tcp://localhost:61616?threadName"));

        // :: The "mats-localvm" fires up an ActiveMQ Broker instance, and creates a ConnectionFactory to that.
        localVmConnectionFactory((springEnvironment) -> new ConnectionFactoryWithStartStopWrapper() {
            private MatsLocalVmActiveMq _matsLocalVmActiveMq;

            @Override
            ConnectionFactory start(String beanName) {
                _matsLocalVmActiveMq = MatsLocalVmActiveMq.createInVmActiveMq(beanName);
                return _matsLocalVmActiveMq.getConnectionFactory();
            }

            @Override
            void stop() {
                _matsLocalVmActiveMq.close();
            }
        });

        // :: Using Default ScenarioDecider
        scenarioDecider(ConfigurableScenarioDecider.getDefaultScenarioDecider());
    }
}
