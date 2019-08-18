package com.stolsvik.mats.spring;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Role;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.MethodIntrospector.MetadataLookup;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.StandardMethodMetadata;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.MatsClassMapping.Stage;

/**
 * The {@link BeanPostProcessor}-class specified by the {@link EnableMats @EnableMats} annotation.
 * <p>
 * It checks all Spring beans in the current Spring {@link ApplicationContext} for whether they have methods annotated
 * with {@link MatsMapping @MatsMapping} or {@link MatsEndpointSetup @MatsEndpointSetup}, and if so configures Mats
 * endpoints for them on the (possibly specified) {@link MatsFactory}. It will also control any registered
 * {@link MatsFactory} beans, invoking {@link MatsFactory#holdEndpointsUntilFactoryIsStarted()} early in the startup
 * procedure before adding the endpoints, and then {@link MatsFactory#start()} as late as possible in the startup
 * procedure, then {@link MatsFactory#stop()} as early as possible in the shutdown procedure.
 *
 * <h3>This is the startup procedure:</h3>
 * <ol>
 * <li>The {@link MatsSpringAnnotationRegistration} (which is a <code>BeanPostProcessor</code>) will have each bean in
 * the Spring ApplicationContext presented:
 * <ol>
 * <li>Each {@link MatsFactory} bean will have their {@link MatsFactory#holdEndpointsUntilFactoryIsStarted()} method
 * invoked.</li>
 * <li>Each bean which will have all their methods searched for the relevant annotations. Such annotated methods will be
 * put in a list.</li>
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
 * any Mats endpoints registered using the Mats SpringConfig. This implies that if you register any Mats endpoints
 * programmatically using e.g. <code>@PostConstruct</code> or similar functionality, these will also be started.
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
 * @author Endre Stølsvik - 2016-05-21 - http://endre.stolsvik.com
 */
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
@Component
public class MatsSpringAnnotationRegistration implements
        BeanPostProcessor,
        ApplicationContextAware {
    // Use clogging, since that's what Spring does.
    private static final Log log = LogFactory.getLog(MatsSpringAnnotationRegistration.class);
    private static final String LOG_PREFIX = "#SPRINGMATS# ";

    private ConfigurableApplicationContext _configurableApplicationContext;
    private ConfigurableListableBeanFactory _configurableListableBeanFactory;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        log.info(LOG_PREFIX + "ApplicationContextAware.setApplicationContext('" + applicationContext.getClass()
                .getSimpleName() + "'): " + applicationContext);
        if (!(applicationContext instanceof ConfigurableApplicationContext)) {
            throw new IllegalStateException("The ApplicationContext when using Mats' SpringConfig"
                    + " must implement " + ConfigurableApplicationContext.class.getSimpleName()
                    + ", while the provided ApplicationContext is of type [" + applicationContext.getClass().getName()
                    + "], and evidently don't.");
        }

        _configurableApplicationContext = (ConfigurableApplicationContext) applicationContext;

        // NOTICE!! We CAN NOT touch the _beans_ at this point, since we then will create them, and we will therefore
        // be hit by the "<bean> is not eligible for getting processed by all BeanPostProcessors" - the
        // BeanPostProcessor in question being ourselves!
        // (.. However, the BeanDefinitions is okay to handle.)
        _configurableListableBeanFactory = _configurableApplicationContext.getBeanFactory();
    }

    private final Map<String, MatsFactory> _matsFactories = new HashMap<>();
    private final IdentityHashMap<MatsFactory, String> _matsFactoriesToName = new IdentityHashMap<>();

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        // ?: Is this bean a MatsFactory?
        if (bean instanceof MatsFactory) {
            // -> Yes, MatsFactory, so ask it to hold any subsequently registered endpoints until the MatsFactory is
            // explicitly started.
            MatsFactory matsFactory = (MatsFactory) bean;
            // ?: However, have we already had the ContextRefreshedEvent presented?
            if (_contextHasBeenRefreshed) {
                // -> Yes, so then we should not hold anyway.
                log.info(LOG_PREFIX + "postProcessBeforeInitialization(bean) invoked, and the bean is a MatsFactory."
                        + " However, the context is already refreshed, so we won't invoke"
                        + " matsFactory.holdEndpointsUntilFactoryIsStarted() - but instead invoke matsFactory.start()."
                        + " This means that any not yet started endpoints will start, and any subsequently registered"
                        + " endpoints will start immediately. The reason why this has happened is most probably due"
                        + " to lazy initialization, where beans are being instantiated \"on demand\" after the "
                        + " life cycle processing has happened (i.e. we got ContextRefreshedEvent already).");
                matsFactory.start();
            }
            else {
                // -> No, have not had ContextRefreshedEvent presented yet - so hold endpoints.
                log.info(LOG_PREFIX + "postProcessBeforeInitialization(bean) invoked, and the bean is a MatsFactory."
                        + " We invoke matsFactory.holdEndpointsUntilFactoryIsStarted(), ensuring that any subsequently"
                        + " registered endpoints is held until we explicitly invoke matsFactory.start() later at"
                        + " ContextRefreshedEvent, so that they do not start processing messages until the entire"
                        + " application is ready for service. We also sets the name to the beanName if not already"
                        + " set.");
                // Ensure that any subsequently registered endpoints won't start until we hit matsFactory.start()
                matsFactory.holdEndpointsUntilFactoryIsStarted();
            }
            // Add to map for later use
            _matsFactories.put(beanName, matsFactory);
            _matsFactoriesToName.put(matsFactory, beanName);
            // Set the name of the MatsFactory to the Spring bean name if it is not already set.
            if ("".equals(matsFactory.getFactoryConfig().getName())) {
                matsFactory.getFactoryConfig().setName(beanName);
            }
        }
        return bean;
    }

    private final Set<Class<?>> _classesWithNoMatsMappingAnnotations = ConcurrentHashMap.newKeySet();

    private static Class<?> getClassOfBean(Object bean) {
        /*
         * There are just too many of these "get the underlying class" stuff in Spring. Since none of these made any
         * difference in the unit tests, I'll go for the one giving the actual underlying "user class", thus having the
         * least amount of added crazy proxy methods to search through. Not sure if this disables any cool Spring magic
         * that could be of interest, e.g. that Spring resolves some meta-annotation stuff by putting the actual
         * annotation on the proxied/generated class or some such.
         */
        // return AopUtils.getTargetClass(bean);
        // return AopProxyUtils.ultimateTargetClass(bean);
        // return bean.getClass(); // Standard Java
        return ClassUtils.getUserClass(bean);
        // Of interest, notice also "AopTestUtils" that does the same thing for Spring beans - the objects themselves.
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // Find actual class of bean (in case of AOPed bean)
        Class<?> targetClass = getClassOfBean(bean);
        // ?: Have we checked this bean before and found no @Mats..-annotations? (might happen with prototype beans)
        if (_classesWithNoMatsMappingAnnotations.contains(targetClass)) {
            // -> Yes, we've checked it before, and it has no @Mats..-annotations.
            return bean;
        }

        // E-> Must check this bean.
        Map<Method, Set<MatsMapping>> methodsWithMatsMappingAnnotations = findRepeatableAnnotatedMethods(targetClass,
                MatsMapping.class);
        Map<Method, Set<MatsEndpointSetup>> methodsWithMatsStagedAnnotations = findRepeatableAnnotatedMethods(
                targetClass, MatsEndpointSetup.class);
        Set<MatsClassMapping> matsEndpointSetupAnnotationsOnClasses = AnnotationUtils.getRepeatableAnnotations(
                targetClass, MatsClassMapping.class);

        // ?: Are there any Mats annotated methods on this class?
        if (methodsWithMatsMappingAnnotations.isEmpty()
                && methodsWithMatsStagedAnnotations.isEmpty()
                && matsEndpointSetupAnnotationsOnClasses.isEmpty()) {
            // -> No, so cache that fact, to short-circuit discovery next time for this class (e.g. prototype beans)
            _classesWithNoMatsMappingAnnotations.add(targetClass);
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "No @MatsMapping, @MatsClassMapping or @MatsEndpointSetup"
                    + " annotations found on bean class [" + bean.getClass() + "].");
            return bean;
        }

        // Assert that it is a singleton
        BeanDefinition beanDefinition = _configurableListableBeanFactory.getBeanDefinition(beanName);
        if (!beanDefinition.isSingleton()) {
            throw new BeanCreationException("The bean [" + beanName + "] is not a singleton, which does not make sense"
                    + " when it comes to beans that has methods annotated with @Mats..-annotations.");
        }

        // ?: Any @MatsMapping annotated methods?
        if (!methodsWithMatsMappingAnnotations.isEmpty()) {
            // -> Yes, there are @MatsMapping annotations. Add them for processing.
            for (Entry<Method, Set<MatsMapping>> entry : methodsWithMatsMappingAnnotations.entrySet()) {
                Method method = entry.getKey();
                for (MatsMapping matsMapping : entry.getValue()) {
                    log.info(LOG_PREFIX + "Found @MatsMapping on method '" + getSimpleMethodDescription(method)
                            + "' :#: Annotation:[" + matsMapping + "] :#: method:[" + method + "].");
                    _matsMappingMethods.add(new MatsMappingHolder(matsMapping, method, bean));
                    // ?: Has context already been refreshed? This might happen if using lazy init, e.g. Remock.
                    if (_contextHasBeenRefreshed) {
                        // -> Yes, already refreshed, so process right away, as the process-at-refresh won't happen.
                        log.info(LOG_PREFIX + " \\- ContextRefreshedEvent already run! Process right away!");
                        processMatsMapping(matsMapping, method, bean);
                    }
                }
            }
        }
        // ?: Any @MatsEndpointSetup annotated methods?
        if (!methodsWithMatsStagedAnnotations.isEmpty()) {
            // -> Yes, there are @MatsEndpointSetup annotations on methods. Add them for processing.
            for (Entry<Method, Set<MatsEndpointSetup>> entry : methodsWithMatsStagedAnnotations.entrySet()) {
                Method method = entry.getKey();
                for (MatsEndpointSetup matsEndpointSetup : entry.getValue()) {
                    log.info(LOG_PREFIX + "Found @MatsMapping on method '" + getSimpleMethodDescription(method)
                            + "' :#: Annotation:[" + matsEndpointSetup + "] :#: method:[" + method + "].");
                    _matsStagedMethods.add(new MatsEndpointSetupHolder(matsEndpointSetup, method, bean));
                    // ?: Has context already been refreshed? This might happen if using lazy init, e.g. Remock.
                    if (_contextHasBeenRefreshed) {
                        // -> Yes, already refreshed, so process right away, as the process-at-refresh won't happen.
                        log.info(LOG_PREFIX + " \\- ContextRefreshedEvent already run! Process right away!");
                        processMatsEndpointSetup(matsEndpointSetup, method, bean);
                    }
                }
            }
        }
        // ?: Any @MatsEndpointSetup annotations on class?
        if (!matsEndpointSetupAnnotationsOnClasses.isEmpty()) {
            // -> Yes, there are @MatsEndpointSetup annotation(s) on class. Add them for processing.
            for (MatsClassMapping matsClassMapping : matsEndpointSetupAnnotationsOnClasses) {
                log.info(LOG_PREFIX + "Found @MatsEndpointSetup on class '"
                        + getClassNameWithoutPackage(ClassUtils.getUserClass(targetClass))
                        + "' :#: Annotation:[" + matsClassMapping + "] :#: class:[" + targetClass + "].");
                _matsStagedClasses.add(new MatsClassMappingHolder(matsClassMapping, bean));
                // ?: Has context already been refreshed? This might happen if using lazy init, e.g. Remock.
                if (_contextHasBeenRefreshed) {
                    // -> Yes, already refreshed, so process right away, as the process-at-refresh won't happen.
                    log.info(LOG_PREFIX + " \\- ContextRefreshedEvent already run! Process right away!");
                    processMatsStagedOnClass(matsClassMapping, bean);
                }
            }
        }
        return bean;
    }

    /**
     * @return a Map [Method, Set-of-Annotations] handling repeatable annotations, e.g. @MatsMapping from the supplied
     *         class, returning an empty map if none.
     */
    private <A extends Annotation> Map<Method, Set<A>> findRepeatableAnnotatedMethods(Class<?> targetClass,
            Class<A> repeatableAnnotationClass) {
        return MethodIntrospector.selectMethods(targetClass, (MetadataLookup<Set<A>>) method -> {
            Set<A> matsMappingAnnotations = AnnotationUtils.getRepeatableAnnotations(method, repeatableAnnotationClass);
            return (!matsMappingAnnotations.isEmpty() ? matsMappingAnnotations : null);
        });
    }

    private final List<MatsMappingHolder> _matsMappingMethods = new ArrayList<>();
    private final List<MatsEndpointSetupHolder> _matsStagedMethods = new ArrayList<>();
    private final List<MatsClassMappingHolder> _matsStagedClasses = new ArrayList<>();

    private static class MatsMappingHolder {
        private final MatsMapping matsMapping;
        private final Method method;
        private final Object bean;

        public MatsMappingHolder(MatsMapping matsMapping, Method method, Object bean) {
            this.matsMapping = matsMapping;
            this.method = method;
            this.bean = bean;
        }
    }

    private static class MatsEndpointSetupHolder {
        private final MatsEndpointSetup matsEndpointSetup;
        private final Method method;
        private final Object bean;

        public MatsEndpointSetupHolder(MatsEndpointSetup matsEndpointSetup, Method method, Object bean) {
            this.matsEndpointSetup = matsEndpointSetup;
            this.method = method;
            this.bean = bean;
        }
    }

    private static class MatsClassMappingHolder {
        private final MatsClassMapping matsClassMapping;
        private final Object bean;

        public MatsClassMappingHolder(MatsClassMapping matsClassMapping, Object bean) {
            this.matsClassMapping = matsClassMapping;
            this.bean = bean;
        }
    }

    private boolean _contextHasBeenRefreshed;

    /**
     * {@link ContextRefreshedEvent} runs pretty much as the latest step in the Spring life cycle starting process:
     * Processes all {@link MatsMapping} and {@link MatsEndpointSetup} annotations, then starts the MatsFactory, which
     * will start any "hanging" MATS Endpoints, which will then start consuming messages.
     */
    @EventListener
    public void onContextRefreshedEvent(ContextRefreshedEvent e) {
        log.info(LOG_PREFIX + "ContextRefreshedEvent: Registering all Endpoints, then running MatsFactory.start()"
                + " on all MatsFactories in the Spring Context to start all registered MATS Endpoints.");

        /*
         * This is a mega-hack to handle the situation where the entire Spring context's bean definitions has been put
         * in lazy-init mode, like "Remock" does. It forces all bean defined MatsFactories to be instantiated if they
         * have not yet been depended on by the beans that so far has been pulled in. This serves two purposes a) They
         * will be registered in the postProcessBeforeInitialization() above, and b) any endpoint whose containing bean
         * is instantiated later (after ContextRefreshedEvent) and which depends on a not-yet instantiated MatsFactory
         * will not crash (as can happen otherwise, as our @MatsFactory Qualification-trickery evidently uses methods of
         * Spring that does not force instantiation of beans, specifically this method:
         * BeanFactoryAnnotationUtils.qualifiedBeanOfType(BeanFactory beanFactory, Class<T> beanType, String qualifier)
         */
        _configurableListableBeanFactory.getBeansOfType(MatsFactory.class);

        /*
         * Now set the flag that denotes that ContextRefreshedEvent has been run, so that any subsequent beans with any
         * of Mats' Spring annotations will be insta-registered - as there won't be a second ContextRefreshedEvent which
         * otherwise has responsibility of registering these (this method). Again, this can happen if the beans in the
         * Spring context are in lazy-init mode.
         */
        _contextHasBeenRefreshed = true;

        // :: Register Mats endpoints for all @MatsMapping and @MatsEndpointSetup annotated methods.
        _matsMappingMethods.forEach(h -> processMatsMapping(h.matsMapping, h.method, h.bean));
        _matsStagedMethods.forEach(h -> processMatsEndpointSetup(h.matsEndpointSetup, h.method, h.bean));
        _matsStagedClasses.forEach(h -> processMatsStagedOnClass(h.matsClassMapping, h.bean));

        // :: Start the MatsFactories
        log.info(LOG_PREFIX + "Invoking matsFactory.start() on all MatsFactories in Spring Context to start"
                + " registered endpoints.");
        _matsFactories.forEach((name, factory) -> {
            log.info(LOG_PREFIX + "  \\- MatsFactory '" + name + "'.start()");
            factory.start();
        });
    }

    /**
     * {@link ContextClosedEvent} runs pretty much as the first step in the Spring life cycle <b>stopping</b> process:
     * Stop the MatsFactory, which will stop all MATS Endpoints, which will have them release their JMS resources - and
     * then the MatsFactory will clean out the JmsMatsJmsSessionHandler.
     */
    @EventListener
    public void onContextClosedEvent(ContextClosedEvent e) {
        log.info(LOG_PREFIX
                + "ContextClosedEvent: Running MatsFactory.stop() on all MatsFactories in the Spring Context"
                + " to stop all registered MATS Endpoints and clean out the JmsMatsJmsSessionHandler.");
        _matsFactories.forEach((name, factory) -> {
            log.info(LOG_PREFIX + "  \\- MatsFactory '" + name + "'.stop()");
            factory.stop();
        });

        // Reset the state. Not that I ever believe this will ever be refreshed again afterwards.
        _contextHasBeenRefreshed = false;
        _matsMappingMethods.clear();
        _matsStagedMethods.clear();
        _matsStagedClasses.clear();
    }

    /**
     * Processes a method annotated with {@link MatsEndpointSetup @MatsMapping} - note that one method can have multiple
     * such annotations, and this method will be invoked for each of them.
     */
    private void processMatsMapping(MatsMapping matsMapping, Method method, Object bean) {
        if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Processing @MatsMapping [" + matsMapping + "] on method ["
                + method + "], of bean: " + bean);

        // ?: Is the endpointId == ""?
        if (matsMapping.endpointId().equals("")) {
            // -> Yes, and it is not allowed to have empty endpointId.
            throw new MatsSpringConfigException("The " + descString(matsMapping, method, bean)
                    + " is missing endpointId (or 'value')");
        }

        // Override accessibility
        method.setAccessible(true);

        // :: Assert that the endpoint is not annotated with @Transactional
        // (Should probably also check whether it has gotten transactional AOP by XML, but who is stupid enough
        // to use XML config in 2016?! Let alone 2018?!)
        Transactional transactionalAnnotation = AnnotationUtils.findAnnotation(method, Transactional.class);
        if (transactionalAnnotation != null) {
            throw new MatsSpringConfigException("The " + descString(matsMapping, method, bean)
                    + " shall not be annotated with @Transactional,"
                    + " as Mats does its own transaction management, method:" + method + ", @Transactional:"
                    + transactionalAnnotation);
        }
        // E-> Good to go, set up the endpoint.

        Parameter[] params = method.getParameters();
        final int paramsLength = params.length;
        int dtoParam = -1;
        int stoParam = -1;
        int processContextParam = -1;
        // ?: Check params
        if (paramsLength == 0) {
            // -> No params, not allowed
            throw new MatsSpringConfigException("The " + descString(matsMapping, method, bean)
                    + " must have at least one parameter: The DTO class.");
        }
        else if (paramsLength == 1) {
            // -> One param, must be the DTO
            // (No need for @Dto-annotation)
            if (params[0].getType().equals(ProcessContext.class)) {
                throw new MatsSpringConfigException("The " + descString(matsMapping, method, bean)
                        + " must have one parameter that is not the"
                        + " ProcessContext: The DTO class");
            }
            dtoParam = 0;
        }
        else {
            // -> More than one param - find each parameter
            // Find the ProcessContext, if present.
            for (int i = 0; i < paramsLength; i++) {
                if (params[i].getType().equals(ProcessContext.class)) {
                    processContextParam = i;
                    break;
                }
            }
            // ?: Was ProcessContext present, and there is only one other param?
            if ((processContextParam != -1) && (paramsLength == 2)) {
                // -> Yes, ProcessContext and one other param: The other shall be the DTO.
                // (No need for @Dto-annotation)
                dtoParam = processContextParam ^ 1;
            }
            else {
                // -> Either ProcessContext + more-than-one extra, or no ProcessContext and more-than-one param.
                // Find DTO and STO, must be annotated with @Dto and @Sto annotations.
                for (int i = 0; i < paramsLength; i++) {
                    if (params[i].getAnnotation(Dto.class) != null) {
                        dtoParam = i;
                    }
                    if (params[i].getAnnotation(Sto.class) != null) {
                        stoParam = i;
                    }
                }
                if (dtoParam == -1) {
                    throw new MatsSpringConfigException("The " + descString(matsMapping, method, bean)
                            + " consists of several parameters, one of which needs to be annotated with @Dto");
                }
            }
        }

        // ----- We've found the parameters.

        // :: Find which MatsFactory to use

        MatsFactory matsFactoryToUse = getMatsFactoryToUse(method, matsMapping.matsFactoryCustomQualifierType(),
                matsMapping.matsFactoryQualifierValue(), matsMapping.matsFactoryBeanName());

        // :: Set up the Endpoint

        Class<?> dtoType = params[dtoParam].getType();
        Class<?> stoType = (stoParam == -1 ? Void.class : params[stoParam].getType());

        // Make the parameters final, since we'll be using them in lambdas
        final int dtoParamF = dtoParam;
        final int processContextParamF = processContextParam;
        final int stoParamF = stoParam;

        final Class<?> replyType = method.getReturnType();

        String typeEndpoint;

        // ?: Do we have a void return value?
        if (replyType.getName().equals("void")) {
            // -> Yes, void return: Setup Terminator.
            typeEndpoint = "Terminator";
            matsFactoryToUse.terminator(matsMapping.endpointId(), stoType, dtoType,
                    (processContext, state, incomingDto) -> {
                        invokeMatsMappingMethod(matsMapping, method, bean, paramsLength,
                                processContextParamF, processContext,
                                dtoParamF, incomingDto,
                                stoParamF, state);
                    });
        }
        else {
            // -> A ReplyType is provided, setup Single endpoint.
            // ?: Is a State parameter specified?
            if (stoParamF != -1) {
                // -> Yes, so then we need to hack together a "single" endpoint out of a staged with single lastStage.
                typeEndpoint = "SingleStage w/State";
                MatsEndpoint<?, ?> ep = matsFactoryToUse.staged(matsMapping.endpointId(), replyType, stoType);
                ep.lastStage(dtoType, (processContext, state, incomingDto) -> {
                    Object reply = invokeMatsMappingMethod(matsMapping, method, bean, paramsLength,
                            processContextParamF, processContext,
                            dtoParamF, incomingDto,
                            stoParamF, state);
                    return helperCast(reply);
                });
            }
            else {
                // -> No state parameter, so use the proper Single endpoint.
                typeEndpoint = "SingleStage";
                matsFactoryToUse.single(matsMapping.endpointId(), replyType, dtoType,
                        (processContext, incomingDto) -> {
                            Object reply = invokeMatsMappingMethod(matsMapping, method, bean, paramsLength,
                                    processContextParamF, processContext,
                                    dtoParamF, incomingDto,
                                    -1, null);
                            return helperCast(reply);
                        });
            }
        }
        if (log.isInfoEnabled()) {
            String procCtxParamDesc = processContextParam != -1 ? "param#" + processContextParam : "<not present>";
            String stoParamDesc = stoParam != -1 ? "param#" + stoParam + ":" + stoType.getSimpleName()
                    : "<not present>";
            String dtoParamDesc = dtoParam != -1 ? "param#" + dtoParam + ":" + dtoType.getSimpleName()
                    : "<not present>";
            log.info(LOG_PREFIX + "Processed " + typeEndpoint + " Mats Spring endpoint by "
                    + descString(matsMapping, method, bean)
                    + " :: ReplyType:[" + replyType.getSimpleName()
                    + "], ProcessContext:[" + procCtxParamDesc
                    + "], STO:[" + stoParamDesc
                    + "], DTO:[" + dtoParamDesc + "]");
        }
    }

    /**
     * Helper for invoking the "method ref" @MatsMapping-annotated method that constitute the Mats process-lambda for
     * SingleStage, SingleStage w/ State, and Terminator mappings.
     */
    private static Object invokeMatsMappingMethod(Annotation matsAnnotation, Method method, Object bean,
            int paramsLength,
            int processContextParamIdx, ProcessContext<?> processContext,
            int dtoParamIdx, Object dto,
            int stoParamIdx, Object sto)
            throws MatsRefuseMessageException {
        Object[] args = new Object[paramsLength];
        if (processContextParamIdx != -1) {
            args[processContextParamIdx] = processContext;
        }
        if (dtoParamIdx != -1) {
            args[dtoParamIdx] = dto;
        }
        if (stoParamIdx != -1) {
            args[stoParamIdx] = sto;
        }
        try {
            return method.invoke(bean, args);
        }
        catch (IllegalAccessException | IllegalArgumentException e) {
            throw new MatsRefuseMessageException("Problem with invoking "
                    + descString(matsAnnotation, method, bean) + ".", e);
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof MatsRefuseMessageException) {
                throw (MatsRefuseMessageException) e.getTargetException();
            }
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new MatsSpringInvocationTargetException("Got InvocationTargetException when invoking "
                    + descString(matsAnnotation, method, bean) + ".", e);
        }
    }

    /**
     * Insane cast helper. <i>Please... help. me.</i>.
     */
    @SuppressWarnings("unchecked")
    private static <R> R helperCast(Object reply) {
        return (R) reply;
    }

    /**
     * Process a method annotated with {@link MatsEndpointSetup @MatsEndpointSetup} - note that one method can have
     * multiple such annotations, and this method will be invoked for each of them.
     */
    private void processMatsEndpointSetup(MatsEndpointSetup matsEndpointSetup, Method method, Object bean) {
        if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Processing @MatsEndpointSetup [" + matsEndpointSetup
                + "] on method ["
                + method + "], of bean: " + bean);

        // ?: Is the endpointId == ""?
        if (matsEndpointSetup.endpointId().equals("")) {
            // -> Yes, and it is not allowed to have empty endpointId.
            throw new MatsSpringConfigException("The " + descString(matsEndpointSetup, method, bean)
                    + " is missing endpointId (or 'value')");
        }

        // Override accessibility
        method.setAccessible(true);

        // Find parameters: MatsEndpoint and potentially EndpointConfig
        Parameter[] params = method.getParameters();
        final int paramsLength = params.length;
        int endpointParam = -1;
        int configParam = -1;
        // ?: Check params
        if (paramsLength == 0) {
            // -> No params, not allowed
            throw new IllegalStateException("The " + descString(matsEndpointSetup, method, bean)
                    + " must have at least one parameter: A MatsEndpoint.");
        }
        else if (paramsLength == 1) {
            // -> One param, must be the MatsEndpoint instances
            if (!params[0].getType().equals(MatsEndpoint.class)) {
                throw new IllegalStateException("The " + descString(matsEndpointSetup, method, bean)
                        + " must have one parameter of type MatsEndpoint.");
            }
            endpointParam = 0;
        }
        else {
            // -> More than one param - find each parameter
            // Find MatsEndpoint and EndpointConfig parameters:
            for (int i = 0; i < paramsLength; i++) {
                if (params[i].getType().equals(MatsEndpoint.class)) {
                    endpointParam = i;
                }
                if (params[i].getType().equals(EndpointConfig.class)) {
                    configParam = i;
                }
            }
            if (endpointParam == -1) {
                throw new IllegalStateException("The " + descString(matsEndpointSetup, method, bean)
                        + " consists of several parameters, one of which needs to be the MatsEndpoint.");
            }
        }

        // ----- We've found the parameters.

        // :: Invoke the @MatsEndpointSetup-annotated staged endpoint setup method

        MatsFactory matsFactoryToUse = getMatsFactoryToUse(method, matsEndpointSetup.matsFactoryCustomQualifierType(),
                matsEndpointSetup.matsFactoryQualifierValue(), matsEndpointSetup.matsFactoryBeanName());
        MatsEndpoint<?, ?> endpoint = matsFactoryToUse
                .staged(matsEndpointSetup.endpointId(), matsEndpointSetup.reply(), matsEndpointSetup.state());

        // Invoke the @MatsEndpointSetup-annotated setup method
        Object[] args = new Object[paramsLength];
        args[endpointParam] = endpoint;
        if (configParam != -1) {
            args[configParam] = endpoint.getEndpointConfig();
        }
        try {
            method.invoke(bean, args);
        }
        catch (IllegalAccessException | IllegalArgumentException e) {
            throw new MatsSpringConfigException("Problem with invoking "
                    + descString(matsEndpointSetup, method, bean) + ".", e);
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new MatsSpringConfigException("Got InvocationTargetException when invoking "
                    + descString(matsEndpointSetup, method, bean) + ".", e);
        }

        log.info(LOG_PREFIX + "Processed Mats Endpoint Configuration by "
                + descString(matsEndpointSetup, method, bean) + " :: MatsEndpoint:[param#" + endpointParam
                + "], EndpointConfig:[param#" + configParam + "]");
    }

    /**
     * Process a class annotated with {@link MatsClassMapping @MatsClassMapping} - note that one class can have multiple
     * such annotations, and this method will be invoked for each of them.
     */
    private void processMatsStagedOnClass(MatsClassMapping matsEndpointSetup, Object bean) {
        // Find actual class of bean (in case of AOPed bean)
        Class<?> targetClass = getClassOfBean(bean);
        log.info("@MatsClassMapping processing!"
                + "\nMatsClassMapping: " + matsEndpointSetup
                + "\nBean: " + bean
                + "\nBean class: " + targetClass);

        // Find all methods annotated with Stage
        Map<Method, Stage> stages = MethodIntrospector.selectMethods(targetClass,
                (MetadataLookup<Stage>) method -> AnnotationUtils.findAnnotation(method, Stage.class));

        // :: Get Stages sorted in order by ordinal, and assert that all are <0 and that no ordinal is duplicated.
        SortedMap<Integer, Method> stagesByOrdinal = new TreeMap<>();
        stages.forEach((method, stageAnnotation) -> {
            int ordinal = stageAnnotation.ordinal();
            // ?: ASSERT: Negative ordinal?
            if (ordinal < 0) {
                // -> Yes, negative -> error.
                throw new MatsSpringConfigException("On @MatsClassMapping endpoint at class '"
                        + getClassNameWithoutPackage(bean) + "', the Stage ordinal is negative (" + ordinal
                        + ") on @Stage annotation of method '" + getSimpleMethodDescription(method)
                        + "' - all ordinals must be >=0 and unique within this endpoint"
                        + " (The ordinal defines the order of stages of this endpoint)."
                        + " @Stage annotation:[" + stageAnnotation + "]");
            }
            // ?: ASSERT: Duplicate ordinal?
            if (stagesByOrdinal.containsKey(ordinal)) {
                // -> Yes, duplicate -> error.
                Method prevMethod = stagesByOrdinal.get(ordinal);
                throw new MatsSpringConfigException("The Stage with ordinal [" + ordinal
                        + "] of @MatsClassMapping endpoint at class '" + getClassNameWithoutPackage(bean)
                        + "' is duplicated on another Stage of the same endpoint."
                        + " All Stages of an endpoint must have ordinal set, and must be unique within the endpoint"
                        + " (The ordinal defines the order of stages of this endpoint)."
                        + "\n  - This method:     '" + getSimpleMethodDescription(method)
                        + "' with @Stage annotation: [" + stageAnnotation + "]"
                        + "\n  - Previous method: '" + getSimpleMethodDescription(prevMethod)
                        + "' with @Stage annotation:[" + stages.get(prevMethod) + "]");
            }
            stagesByOrdinal.put(ordinal, method);
        });
        // ?: ASSERT: Do we have an INITIAL Stage?
        if (!stagesByOrdinal.containsKey(0)) {
            // -> No, INITIAL Stage not present -> error.
            throw new MatsSpringConfigException("The @MatsClassMapping endpoint at class '"
                    + getClassNameWithoutPackage(bean) + "' is missing initial stage:"
                    + " No method is annotated with '@Stage(Stage.INITIAL)' (i.e. ordinal=0)");
        }

        // :: ASSERT: Check that only the last stage has a return type
        int lastOrdinal = stagesByOrdinal.lastKey();
        for (Entry<Integer, Method> entry : stagesByOrdinal.entrySet()) {
            int ordinal = entry.getKey();
            Method method = entry.getValue();
            // ?: Is this the last ordinal?
            if (ordinal == lastOrdinal) {
                // -> Last, so ignore.
                continue;
            }
            // ?: ASSERT: Does this non-last Stage return something else than void?
            if (method.getReturnType() != void.class) {
                // -> Yes, returns non-void -> error.
                throw new MatsSpringConfigException("The Stage with ordinal [" + ordinal
                        + "] of @MatsClassMapping endpoint at class '" + getClassNameWithoutPackage(bean)
                        + "' has a return type '" + getClassNameWithoutPackage(method.getReturnType())
                        + "' (not void), but it is not the last stage. Only the last stage shall have a return type,"
                        + " which is the return type for the endpoint. Method: '"
                        + getSimpleMethodDescription(method) + "'");
            }
        }

        // Find reply type: Get return type of last Stage
        Method lastStageMethod = stagesByOrdinal.get(stagesByOrdinal.lastKey());
        // .. modify replyClass if it is void.class to Void.class.. Nuances.. They matter..
        Class<?> replyClass = lastStageMethod.getReturnType() == void.class
                ? Void.class
                : lastStageMethod.getReturnType();

        // :: Find request type: Get DTO-type of first Stage

        Method initialStageMethod = stagesByOrdinal.get(stagesByOrdinal.firstKey());
        int dtoParamPosOfInitial = findDtoPosition(initialStageMethod);
        if (dtoParamPosOfInitial == -1) {
            throw new MatsSpringConfigException("The Initial Stage of @MatsClassMapping endpoint at class '"
                    + getClassNameWithoutPackage(bean) + "' does not have a incoming DTO (message) parameter."
                    + " Either it must be the sole parameter, or it must be marked by annotation @Dto."
                    + " Method: [" + initialStageMethod + "]");
        }
        Class<?> requestClass = initialStageMethod.getParameters()[dtoParamPosOfInitial].getType();

        log.info(LOG_PREFIX + "The @MatsClassMapping endpoint at class '" + getClassNameWithoutPackage(targetClass)
                + "' has request DTO [" + getClassNameWithoutPackage(requestClass)
                + "] and reply DTO [" + getClassNameWithoutPackage(replyClass) + "].");

        // :: Find all non-null fields of the bean - these are what Spring has injected.
        Field[] processContextField_hack = new Field[1];
        LinkedHashMap<Field, Object> templateFields = new LinkedHashMap<>();
        ReflectionUtils.doWithFields(targetClass, field -> {
            String name = field.getName();
            Class<?> clazz = field.getType();
            if (clazz.isPrimitive()) {
                log.info(LOG_PREFIX + " - Field [" + name + "] is primitive (" + clazz
                        + "): Assuming state field, ignoring.");
                return;
            }
            if (clazz.isAssignableFrom(ProcessContext.class)) {
                // ProcessContext shall return a ParameterizedType, as it has a type parameter
                ParameterizedType genericType = (ParameterizedType) field.getGenericType();
                Type typeArgument = genericType.getActualTypeArguments()[0];
                log.info(LOG_PREFIX + " - Field [" + name + "] is Mats' ProcessContext<..>, with reply type ["
                        + typeArgument + "]");
                if (processContextField_hack[0] != null) {
                    throw new MatsSpringConfigException("The @MatsClassMapping endpoint at class '"
                            + getClassNameWithoutPackage(bean) + "' evidently has more than one ProcessContext field."
                            + " Only one is allowed."
                            + "\n  - This field:     [" + field + "]"
                            + "\n  - Previous field: [" + processContextField_hack[0] + "]");
                }
                if (typeArgument != replyClass) {
                    throw new MatsSpringConfigException("The @MatsClassMapping endpoint at class '"
                            + getClassNameWithoutPackage(bean) + "' has a ProcessContext field where the reply type"
                            + " does not match the resolved reply type from the Stages."
                            + " ProcessContext Field: [" + field + "]"
                            + "\n  - Type from field:   ProcessContext<" + typeArgument + ">"
                            + "\n  - Reply type resolved from Stages: [" + replyClass + "]");
                }
                processContextField_hack[0] = field;
                return;
            }

            field.setAccessible(true);
            Object value = field.get(bean);
            if (value == null) {
                log.info(LOG_PREFIX + " - Field [" + name + "] is null: Assuming state field, ignoring. (Type: ["
                        + field.getGenericType() + "])");
                return;
            }

            // ----- This is a Spring Dependency Injected field. We assume.
            // (Or the idiot developer initialized a state field with a value. Please explain me why this makes sense.)

            log.info(LOG_PREFIX + " - Field [" + name + "] is non-null: Assuming Spring Dependency Injection has set it"
                    + " - storing as template. (Type:[" + field.getGenericType() + "], Value:[" + value + "])");
            templateFields.put(field, value);
        });

        Field processContextField = processContextField_hack[0];
        processContextField.setAccessible(true);

        // :: Find which MatsFactory to use

        MatsFactory matsFactoryToUse = getMatsFactoryToUse(targetClass, matsEndpointSetup
                .matsFactoryCustomQualifierType(),
                matsEndpointSetup.matsFactoryQualifierValue(), matsEndpointSetup.matsFactoryBeanName());

        // :: Create the Staged Endpoint

        MatsEndpoint<?, ?> ep = matsFactoryToUse.staged(matsEndpointSetup.endpointId(), replyClass, targetClass);

        stagesByOrdinal.forEach((ordinal, method) -> {
            log.info(LOG_PREFIX + "  -> Stage '" + ordinal + "': '" + getSimpleMethodDescription(method) + "");
            int dtoParamPos = findDtoPosition(method);
            Parameter[] parameters = method.getParameters();
            Class<?> incomingClass = dtoParamPos == -1
                    ? Void.class
                    : parameters[dtoParamPosOfInitial].getType();
            int paramsLength = parameters.length;

            ep.stage(incomingClass, (processContext, state, incomingDto) -> {
                for (Entry<Field, Object> entry : templateFields.entrySet()) {
                    Field templateField = entry.getKey();
                    try {
                        templateField.set(state, entry.getValue());
                    }
                    catch (IllegalAccessException e) {
                        throw new MatsSpringInvocationTargetException("Didn't manage to set \"template field\" '"
                                + templateField.getName() + "' assumed coming from Spring Dependency Injection into the "
                                + " @MatsClassMapping combined state/@Service class upon invocation of Mats Stage.", e);
                    }
                }

                // :: Set the ProcessContext for this processing.

                try {
                    processContextField.set(state, processContext);
                }
                catch (IllegalAccessException e) {
                    throw new MatsSpringInvocationTargetException("Didn't manage to set the ProcessContext '"
                            + processContextField.getName() + "' into the "
                            + " @MatsClassMapping combined state/@Service class upon invocation of Mats Stage.", e);
                }

                // :: Make the invocation
                Object o = invokeMatsMappingMethod(matsEndpointSetup, method, state, paramsLength,
                        -1, null,
                        dtoParamPos, incomingDto,
                        -1, null);

                // :: Null out the template fields again, before serializing the state.
                for (Entry<Field, Object> entry : templateFields.entrySet()) {
                    Field templateField = entry.getKey();
                    try {
                        templateField.set(state, null);
                    }
                    catch (IllegalAccessException e) {
                        throw new MatsSpringInvocationTargetException("Didn't manage to null \"template field\" '"
                                + templateField.getName() + "'.", e);
                    }
                }

                // :: Null out the ProcessContext
                try {
                    processContextField.set(state, null);
                }
                catch (IllegalAccessException e) {
                    throw new MatsSpringInvocationTargetException("Didn't manage to null the ProcessContext '"
                            + processContextField.getName() + "'.", e);
                }

                log.info("Testing 1, 2, 3");

                // ?: Is this the last stage?
                if (method == lastStageMethod) {
                    // -> Yes, this is the last stage - so return whatever the invocation came up with.
                    processContext.reply(helperCast(o));
                }
            });
        });

    }

    private int findDtoPosition(Method method) {
        Parameter[] parameters = method.getParameters();
        int dtoParamPos = -1;
        if (parameters.length == 1) {
            dtoParamPos = 0;
        }
        else {
            for (int i = 0; i < parameters.length; i++) {
                if (parameters[i].getAnnotation(Dto.class) != null) {
                    if (dtoParamPos != -1) {
                        throw new MatsSpringConfigException("More than one parameter of method '"
                                + getSimpleMethodDescription(method) + "' is annotated with @Dto");
                    }
                    dtoParamPos = i;
                }
            }
        }
        return dtoParamPos;
    }

    private MatsFactory getMatsFactoryToUse(AnnotatedElement annotatedElement,
            Class<? extends Annotation> aeCustomQualifierType,
            String aeQualifierValue, String aeBeanName) {
        List<MatsFactory> specifiedMatsFactories = new ArrayList<>();

        int numberOfQualifications = 0;

        Annotation[] annotations = AnnotationUtils.getAnnotations(annotatedElement);
        for (Annotation annotation : annotations) {
            // ?: Is this a @Qualifier
            if (annotation.annotationType() == Qualifier.class) {
                // -> Yes, @Qualifier - so get the correct MatsFactory by Qualifier.value()
                Qualifier qualifier = (Qualifier) annotation;
                specifiedMatsFactories.add(getMatsFactoryByQualifierValue(qualifier.value()));
                numberOfQualifications++;
            }
            // ?: Is this a custom qualifier annotation, meta-annotated with @Qualifier?
            else if (AnnotationUtils.isAnnotationMetaPresent(annotation.annotationType(),
                    Qualifier.class)) {
                // -> Yes, @Qualifier-meta-annotated annotation - get the correct MatsFactory also annotated with this
                specifiedMatsFactories.add(getMatsFactoryByCustomQualifier(annotation.annotationType(), annotation));
                numberOfQualifications++;
            }
        }

        // Was the annotation element 'matsFactoryCustomQualifierType' specified?
        if (aeCustomQualifierType != Annotation.class) {
            specifiedMatsFactories.add(getMatsFactoryByCustomQualifier(aeCustomQualifierType, null));
            numberOfQualifications++;
        }

        // ?: Was the annotation element 'matsFactoryQualifierValue' specified?
        if (!"".equals(aeQualifierValue)) {
            specifiedMatsFactories.add(getMatsFactoryByQualifierValue(aeQualifierValue));
            numberOfQualifications++;
        }

        // ?: Was the annotation element 'matsFactoryQualifierValue' specified?
        if (!"".equals(aeBeanName)) {
            specifiedMatsFactories.add(getMatsFactoryByBeanName(aeBeanName));
            numberOfQualifications++;
        }

        // ?: Was more than one qualification style in use?
        if (numberOfQualifications > 1) {
            // -> Yes, and that's an ambiguous qualification, which we don't like.
            throw new BeanCreationException("When trying to get specific MatsFactory based on @Mats..-annotation"
                    + " properties; and @Qualifier-annotations and custom qualifier annotations on the"
                    + " @Mats..-annotated method, we found that there was more than one qualification style present."
                    + " Check your specifications on the element [" + annotatedElement + "].");
        }

        // ?: Did the specific MatsFactory logic end up with more than one MatsFactory?
        if (specifiedMatsFactories.size() > 1) {
            // -> Yes, and that's an ambiguous qualification, which we don't like.
            throw new BeanCreationException("When trying to get specific MatsFactory based on @Mats..-annotation"
                    + " properties; and @Qualifier-annotations and custom qualifier annotations on the"
                    + " @Mats..-annotated method, we ended up with more than one MatsFactory."
                    + " Check your specifications on the element [" + annotatedElement + "].");
        }
        // ?: If there was one specified MatsFactory, use that, otherwise find any MatsFactory (fails if more than one).
        MatsFactory matsFactory = specifiedMatsFactories.size() == 1
                ? specifiedMatsFactories.get(0)
                : getMatsFactoryUnspecified();

        if (log.isDebugEnabled()) log.debug(LOG_PREFIX + ".. using MatsFactory ["
                + _matsFactoriesToName.get(matsFactory) + "]: [" + matsFactory + "].");

        return matsFactory;
    }

    /**
     * Using lazy getting of MatsFactory, as else we can get a whole heap of <i>"Bean of type is not eligible for
     * getting processed by all BeanPostProcessors (for example: not eligible for auto-proxying)"</i> situations.
     */
    private MatsFactory getMatsFactoryUnspecified() {
        try {
            return _configurableApplicationContext.getBean(MatsFactory.class);
        }
        catch (NoUniqueBeanDefinitionException e) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there was MULTIPLE MatsFactories available in the"
                    + " Spring ApplicationContext - You must specify which one to use: Using props on the"
                    + " @Mats..-annotation itself (read its JavaDoc); or further annotate the @Mats..-annotated method"
                    + " with a @Qualifier(value), which either matches the same @Qualifier(value)-annotation on a"
                    + " MatsFactory bean, or where the 'value' matches the bean name of a MatsFactory; or annotate both"
                    + " the @Mats..-annotated method and the MatsFactory @Bean factory method with the same custom"
                    + " annotation which is meta-annotated with @Qualifier; or mark one (and only one) of the"
                    + " MatsFactories as @Primary", e);
        }
        catch (NoSuchBeanDefinitionException e) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there is no MatsFactory available in the"
                    + " Spring ApplicationContext", e);
        }
    }

    private final Map<String, MatsFactory> _cache_MatsFactoryByBeanName = new HashMap<>();

    private MatsFactory getMatsFactoryByBeanName(String beanName) {
        // :: Cache lookup
        MatsFactory matsFactory = _cache_MatsFactoryByBeanName.get(beanName);
        if (matsFactory != null) {
            return matsFactory;
        }
        // E-> Didn't find in cache, see if we can find it now.
        try {
            Object bean = _configurableApplicationContext.getBean(beanName);
            if (!(bean instanceof MatsFactory)) {
                throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                        .getClass().getSimpleName() + " found that the @Mats..-annotation specified Spring bean '"
                        + beanName + "' is not of type MatsFactory");
            }
            // Cache, and return
            _cache_MatsFactoryByBeanName.put(beanName, (MatsFactory) bean);
            return (MatsFactory) bean;
        }
        catch (NoSuchBeanDefinitionException e) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there is no MatsFactory with the name '" + beanName
                    + "' available in the Spring ApplicationContext", e);
        }
    }

    private final Map<String, MatsFactory> _cache_MatsFactoryByQualifierValue = new HashMap<>();

    private MatsFactory getMatsFactoryByQualifierValue(String qualifierValue) {
        // :: Cache lookup
        MatsFactory matsFactory = _cache_MatsFactoryByQualifierValue.get(qualifierValue);
        if (matsFactory != null) {
            return matsFactory;
        }
        // E-> Didn't find in cache, see if we can find it now.
        try {
            matsFactory = BeanFactoryAnnotationUtils.qualifiedBeanOfType(_configurableListableBeanFactory,
                    MatsFactory.class, qualifierValue);
            // Cache, and return
            _cache_MatsFactoryByQualifierValue.put(qualifierValue, matsFactory);
            return matsFactory;
        }
        catch (NoUniqueBeanDefinitionException e) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there was MULTIPLE MatsFactories available in the"
                    + " Spring ApplicationContext with the qualifier value '" + qualifierValue + "', this is probably"
                    + " not what you want.", e);
        }
        catch (NoSuchBeanDefinitionException e) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there is no MatsFactory with the qualifier value '"
                    + qualifierValue + "' available in the Spring ApplicationContext", e);
        }
    }

    private final Map<Class<? extends Annotation>, Map<Annotation, MatsFactory>> _cache_MatsFactoryByCustomQualifier = new HashMap<>();

    /**
     * @param customQualifierType
     *            must be non-null.
     * @param customQualifier
     *            If != null, then it must be .equals() with the one on the bean, otherwise it is enough that the type
     *            mathces.
     * @return the matched bean - if more than one bean matches, {@link BeanCreationException} is raised.
     */
    private MatsFactory getMatsFactoryByCustomQualifier(Class<? extends Annotation> customQualifierType,
            Annotation customQualifier) {
        // :: Cache lookup
        Map<Annotation, MatsFactory> subCacheMap = _cache_MatsFactoryByCustomQualifier.get(customQualifierType);
        if (subCacheMap != null) {
            MatsFactory matsFactory = subCacheMap.get(customQualifier);
            if (matsFactory != null) {
                return matsFactory;
            }
        }
        // E-> Didn't find in cache, see if we can find it now.

        // :: First find all beans that are annotated with the custom qualifier and are of type MatsFactory.
        // (This should really not exist, as it would mean that you've extended e.g. JmsMatsFactory and annotated that,
        // which is really not something I am advocating.)
        String[] beanNamesWithCustomQualifierClass = _configurableListableBeanFactory.getBeanNamesForAnnotation(
                customQualifierType);
        ArrayList<Object> annotatedBeans = Arrays.stream(beanNamesWithCustomQualifierClass)
                .filter(beanName -> {
                    // These beans HAVE the custom qualifier type present, so this method will not return null.
                    Annotation annotationOnBean = _configurableListableBeanFactory.findAnnotationOnBean(beanName,
                            customQualifierType);
                    // If the custom qualifier instance is not specified, then it is enough that the type matches.
                    // If specified, the annotation instance must be equal to the one on the bean.
                    return customQualifier == null || customQualifier.equals(annotationOnBean);
                })
                .map(name -> _configurableListableBeanFactory.getBean(name))
                .collect(Collectors.toCollection(ArrayList::new));

        // :: Then find all MatsFactories created by @Bean factory methods, which are annotated with custom qualifier
        // (This is pretty annoyingly not a feature that is easily available in Spring proper.)
        String[] beanDefinitionNames = _configurableListableBeanFactory.getBeanDefinitionNames();
        for (String beanDefinitionName : beanDefinitionNames) {
            BeanDefinition beanDefinition = _configurableListableBeanFactory.getBeanDefinition(beanDefinitionName);
            // ?: Is this an AnnotatedBeanDefinition, thus might be a bean created by a factory method (e.g. @Bean)
            if (beanDefinition instanceof AnnotatedBeanDefinition) {
                // -> Yes, AnnotatedBeanDefinition.
                // ?: Is it a factory method?
                MethodMetadata factoryMethodMetadata = ((AnnotatedBeanDefinition) beanDefinition)
                        .getFactoryMethodMetadata();
                if (factoryMethodMetadata != null) {
                    // -> Yes, factory method
                    // ?: Can we introspect that method?
                    if (!(factoryMethodMetadata instanceof StandardMethodMetadata)) {
                        // -> No, so just skip it after logging.
                        log.warn(LOG_PREFIX + "AnnotatedBeanDefinition.getFactoryMethodMetadata() returned a"
                                + " MethodMetadata which is not of type StandardMethodMetadata - therefore cannot run"
                                + " getIntrospectedMethod() on it to find annotations on the factory method."
                                + " AnnotatedBeanDefinition: [" + beanDefinition + "],"
                                + " MethodMetadata: [" + factoryMethodMetadata + "]");
                        continue;
                    }
                    StandardMethodMetadata factoryMethodMetadata_Standard = (StandardMethodMetadata) factoryMethodMetadata;

                    Method introspectedMethod = factoryMethodMetadata_Standard.getIntrospectedMethod();
                    Annotation[] annotations = introspectedMethod.getAnnotations();
                    for (Annotation annotation : annotations) {
                        // If the annotation instance is not specified, then it is enough that the type matches.
                        // If specified, the annotation instance must be equal to the one on the bean.
                        if (((customQualifier == null) && (annotation.annotationType() == customQualifierType))
                                || annotation.equals(customQualifier)) {
                            annotatedBeans.add(_configurableListableBeanFactory.getBean(beanDefinitionName));
                        }
                    }
                }
            }
        }

        // :: Filter only the MatsFactories, log informational if found beans that are not MatsFactory.
        List<MatsFactory> matsFactories = new ArrayList<>();
        for (Object annotatedBean : annotatedBeans) {
            if (!(annotatedBean instanceof MatsFactory)) {
                log.info(LOG_PREFIX + "Found bean annotated with correct custom qualifier [" + customQualifierType
                        + "], but it was not a MatsFactory. Ignoring. Bean: [" + annotatedBean + "].");
                continue;
            }
            matsFactories.add((MatsFactory) annotatedBean);
        }

        String qualifierString = (customQualifier != null ? customQualifier.toString()
                : customQualifierType.getSimpleName());
        if (matsFactories.size() > 1) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there was MULTIPLE MatsFactories available in the"
                    + " Spring ApplicationContext with the custom qualifier annotation '" + qualifierString
                    + "', this is probably not what you want.");
        }
        if (matsFactories.isEmpty()) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there is no MatsFactory with the custom qualifier"
                    + " annotation '" + qualifierString + "' available in the Spring ApplicationContext");
        }
        // Cache, and return
        _cache_MatsFactoryByCustomQualifier.computeIfAbsent(customQualifierType, $ -> new HashMap<>())
                .put(customQualifier, matsFactories.get(0));
        return matsFactories.get(0);
    }

    /**
     * Thrown if the setup of a Mats Spring endpoint fails.
     */
    public static class MatsSpringConfigException extends RuntimeException {
        public MatsSpringConfigException(String message, Throwable cause) {
            super(message, cause);
        }

        public MatsSpringConfigException(String message) {
            super(message);
        }
    }

    /**
     * Thrown if the invocation of a {@link MatsMapping @MatsMapping} or {@link MatsEndpointSetup @MatsEndpointSetup}
     * annotated method raises {@link InvocationTargetException} and the underlying exception is not a
     * {@link RuntimeException}.
     */
    public static class MatsSpringInvocationTargetException extends RuntimeException {
        public MatsSpringInvocationTargetException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static String getClassNameWithoutPackage(Object object) {
        if (object == null) {
            return "<null instance>";
        }
        return getClassNameWithoutPackage(object.getClass());
    }

    private static String getClassNameWithoutPackage(Class<?> clazz) {
        if (clazz == null) {
            return "<null class>";
        }
        if (clazz == void.class) {
            return "void";
        }
        String typeName = ClassUtils.getUserClass(clazz).getTypeName();
        String packageName = clazz.getPackage().getName();
        return typeName.replace(packageName + ".", "");
    }

    private static String getSimpleMethodDescription(Method method) {
        return method.getReturnType().getSimpleName() + ' ' + getClassNameWithoutPackage(method.getDeclaringClass())
                + '.' + method.getName() + "(..)";
    }

    private static String descString(Annotation annotation, Method method, Object bean) {
        return "@" + annotation.annotationType().getSimpleName() + "-annotated method '"
                + getSimpleMethodDescription(method) + '\'';
    }
}
