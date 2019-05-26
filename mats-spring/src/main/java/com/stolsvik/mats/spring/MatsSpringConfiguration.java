package com.stolsvik.mats.spring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.SpringVersion;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsFactory;

/**
 * The {@link Configuration}-class specified by the {@link EnableMats @EnableMats} annotation.
 * <p>
 * It creates a {@link MatsSpringAnnotationRegistration} bean that checks all Spring beans in the current Spring
 * {@link ApplicationContext} for whether they have methods annotated with {@link MatsMapping @MatsMapping} or
 * {@link MatsStaged @MatsStaged}, and if so configures Mats endpoints for them on the (possibly specified)
 * {@link MatsFactory}. It will also control any registered {@link MatsFactory} beans, invoking
 * {@link MatsFactory#holdEndpointsUntilFactoryIsStarted()} and {@link MatsFactory#start()} in the startup procedure,
 * then {@link MatsFactory#stop()} in the shutdown procedure.
 * 
 * <h3>This is the startup procedure:</h3>
 * <ol>
 * <li>The {@link MatsSpringAnnotationRegistration} BeanPostProcessor will have each bean in the Spring
 * ApplicationContext presented:
 * <ol>
 * <li>Each {@link MatsFactory} bean will have their {@link MatsFactory#holdEndpointsUntilFactoryIsStarted()} method
 * invoked.</li>
 * <li>Each beans which will have all their methods searched for the relevant annotations. Such annotated methods will
 * be put in a list.</li>
 * </ol>
 * </li>
 * <li>Upon {@link ContextRefreshedEvent}:
 * <ol>
 * <li>All definitions that was put in the list will be processed, and Mats Endpoints will be registered into the
 * MatsFactory - using the specified MatsFactory if this was qualified in the definition (Read more at
 * {@link MatsMapping @MatsMapping}).</li>
 * <li>Then, all MatsFactories present in the ApplicationContext will have their {@link MatsFactory#start()} method
 * invoked, which will "release" the configured Mats endpoints, so that they will start (which in turn fires up their
 * "stage processor" threads, which will each go and get a connection to the underlying MQ (which probably is JMS-based)
 * and start to listen for messages).</li>
 * </ol>
 * </li>
 * </ol>
 * Do notice that <i>all</i> MatsFactories in the Spring ApplicationContext are started, regardless of whether they had
 * any Mats endpoints registered.
 *
 * <h3>This is the shutdown procedure:</h3>
 * <ol>
 * <li>Upon {@link ContextClosedEvent}, all MatsFactories in the ApplicationContext will have their
 * {@link MatsFactory#stop()} method invoked.
 * <li>This causes all registered Mats Endpoints to be {@link MatsEndpoint#stop() stopped}, which releases the
 * connection to the underlying MQ and stops the stage processor threads.
 * </ol>
 * Notice: ContextClosedEvent is fired rather early in the Spring ApplicationContext shutdown procedure. When running in
 * integration testing mode, which typically will involve starting an in-vm ActiveMQ in the same JVM as the endpoints,
 * this early stopping and releasing of connections is beneficial so that when the in-vm ActiveMQ is stopped somewhat
 * later, one won't get a load of connection failures from the Mats endpoints which otherwise would have their
 * connections shut down under their feet.
 * 
 * 
 * @author Endre Stølsvik - 2016-05-21 - http://endre.stolsvik.com
 */
@Configuration
public class MatsSpringConfiguration {
    public static final String LOG_PREFIX = "#SPRINGMATS# ";

    // Use clogging, since that's what Spring does.
    private static final Log log = LogFactory.getLog(MatsSpringAnnotationRegistration.class);

    @Bean
    public MatsSpringAnnotationRegistration matsSpringAnnotationBeanPostProcessor() {
        log.info(LOG_PREFIX + "SpringVersion: " + SpringVersion.getVersion() + " - Instantiating "
                + MatsSpringAnnotationRegistration.class.getSimpleName());
        return new MatsSpringAnnotationRegistration();
    }
}
