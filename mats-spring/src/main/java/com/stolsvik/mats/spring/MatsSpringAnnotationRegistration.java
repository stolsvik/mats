package com.stolsvik.mats.spring;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.support.AopUtils;
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

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.spring.MatsMapping.MatsMappings;
import com.stolsvik.mats.spring.MatsStaged.MatsStageds;

/**
 * The {@link BeanPostProcessor}-class specified by the {@link EnableMats @EnableMats} annotation.
 * <p>
 * It checks all Spring beans in the current Spring {@link ApplicationContext} for whether they have methods annotated
 * with {@link MatsMapping @MatsMapping} or {@link MatsStaged @MatsStaged}, and if so configures Mats endpoints for them
 * on the (possibly specified) {@link MatsFactory}. It will also control any registered {@link MatsFactory} beans,
 * invoking {@link MatsFactory#holdEndpointsUntilFactoryIsStarted()} early in the startup procedure before adding the
 * endpoints, and then {@link MatsFactory#start()} as late as possible in the startup procedure, then
 * {@link MatsFactory#stop()} as early as possible in the shutdown procedure.
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

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // Find actual class of bean (in case of AOPed bean)
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        // ?: Have we checked this bean before and found no @Mats..-annotations? (might happen with prototype beans)
        if (_classesWithNoMatsMappingAnnotations.contains(targetClass)) {
            // -> Yes, we've checked it before, and it has no @Mats..-annotations.
            return bean;
        }

        // E-> Must check this bean.
        Map<Method, Set<MatsMapping>> methodsWithMatsMappingAnnotations = findAnnotatedMethods(targetClass,
                MatsMapping.class, MatsMappings.class);
        Map<Method, Set<MatsStaged>> methodsWithMatsStagedAnnotations = findAnnotatedMethods(targetClass,
                MatsStaged.class, MatsStageds.class);

        // ?: Are there any Mats annotated methods on this class?
        if (methodsWithMatsMappingAnnotations.isEmpty() && methodsWithMatsStagedAnnotations.isEmpty()) {
            // -> No, so cache that fact, to short-circuit discovery next time for this class (e.g. prototype beans)
            _classesWithNoMatsMappingAnnotations.add(targetClass);
            if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "No @MatsMapping or @MatsStaged annotations found"
                    + " on bean class [" + bean.getClass() + "].");

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
                    log.info(LOG_PREFIX + "Found @MatsMapping [" + matsMapping + "] on method [" + method + "].");
                    _matsMappings.add(new MatsMappingHolder(matsMapping, method, bean));
                    if (_contextHasBeenRefreshed) {
                        log.info(LOG_PREFIX + " \\- ContextRefreshedEvent already run! Process right away!");
                        processMatsMapping(matsMapping, method, bean);
                    }
                }
            }
        }
        // ?: Any @MatsStaged annotated methods?
        if (!methodsWithMatsStagedAnnotations.isEmpty()) {
            // -> Yes, there are @MatsStaged annotations. Add them for processing.
            for (Entry<Method, Set<MatsStaged>> entry : methodsWithMatsStagedAnnotations.entrySet()) {
                Method method = entry.getKey();
                for (MatsStaged matsStaged : entry.getValue()) {
                    log.info(LOG_PREFIX + "Found @MatsStaged [" + matsStaged + "] on method [" + method + "].");
                    _matsStageds.add(new MatsStagedHolder(matsStaged, method, bean));
                    if (_contextHasBeenRefreshed) {
                        log.info(LOG_PREFIX + " \\- ContextRefreshedEvent already run! Process right away!");
                        processMatsStaged(matsStaged, method, bean);
                    }
                }
            }
        }
        return bean;
    }

    /**
     * @return a Map [Method, Set-of-Annotations] handling repeatable annotations, e.g. @MatsMapping from the supplied
     *         class, returning an empty map if none.
     */
    private <A extends Annotation> Map<Method, Set<A>> findAnnotatedMethods(Class<?> targetClass,
            Class<A> single, Class<? extends Annotation> plural) {
        return MethodIntrospector.selectMethods(targetClass, (MetadataLookup<Set<A>>) method -> {
            Set<A> matsMappingAnnotations = AnnotationUtils.getRepeatableAnnotations(method, single, plural);
            return (!matsMappingAnnotations.isEmpty() ? matsMappingAnnotations : null);
        });
    }

    private final List<MatsMappingHolder> _matsMappings = new ArrayList<>();
    private final List<MatsStagedHolder> _matsStageds = new ArrayList<>();

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

    private static class MatsStagedHolder {
        private final MatsStaged matsStaged;
        private final Method method;
        private final Object bean;

        public MatsStagedHolder(MatsStaged matsStaged, Method method, Object bean) {
            this.matsStaged = matsStaged;
            this.method = method;
            this.bean = bean;
        }
    }

    private boolean _contextHasBeenRefreshed;

    /**
     * {@link ContextRefreshedEvent} runs pretty much as the latest step in the Spring life cycle starting process:
     * Processes all {@link MatsMapping} and {@link MatsStaged} annotations, then starts the MatsFactory, which will
     * start any "hanging" MATS Endpoints, which will then start consuming messages.
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
         * will not crash (as can happen otherwise, as our @MatsMapping Qualification-trickery evidently uses methods of
         * Spring that does not force instantiation of beans, specifically this method:
         * BeanFactoryAnnotationUtils.qualifiedBeanOfType(BeanFactory beanFactory, Class<T> beanType, String qualifier)
         */
        _configurableListableBeanFactory.getBeansOfType(MatsFactory.class);

        // Now set the flag that denotes that ContextRefreshedEvent has been run, so that any subsequent beans with
        // @MatsMapping and friends will be insta-registered (as there won't be a second run of the present method,
        // which otherwise has responsibility of registering these).
        _contextHasBeenRefreshed = true;

        // :: Register Mats endpoints for all @MatsMapping and @MatsStaged annotated methods.
        _matsMappings.forEach(h -> processMatsMapping(h.matsMapping, h.method, h.bean));
        _matsStageds.forEach(h -> processMatsStaged(h.matsStaged, h.method, h.bean));

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
        _matsMappings.clear();
        _matsStageds.clear();
    }

    /**
     * Processes a method annotated with {@link MatsStaged @MatsMapping} - note that one method can have multiple such
     * annotations, and this method will be invoked for each of them.
     */
    private void processMatsMapping(MatsMapping matsMapping, Method method, Object bean) {
        if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Processing @MatsMapping [" + matsMapping + "] on method ["
                + method + "], of bean: " + bean);

        // Override accessibility
        method.setAccessible(true);

        // :: Assert that the endpoint is not annotated with @Transactional
        // (Should probably also check whether it has gotten transactional AOP by XML, but who is stupid enough
        // to use XML config in 2016?! Let alone 2018?!)
        Transactional transactionalAnnotation = AnnotationUtils.findAnnotation(method, Transactional.class);
        if (transactionalAnnotation != null) {
            throw new MatsSpringConfigException("The " + descString(matsMapping, method, bean)
                    + " shall not be annotated with @Transactional,"
                    + " as it does its own transaction management, method:" + method + ", @Transactional:"
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
                        invokeMatsMappingMethod(matsMapping, method, bean,
                                paramsLength, processContextParamF,
                                processContext, dtoParamF,
                                incomingDto, stoParamF, state);
                    });
        }
        else {
            // -> A ReplyType is provided, setup Single endpoint.
            // ?: Is a State parameter specified?
            if (stoParamF != -1) {
                // -> Yes, so then we need to hack together a "single" endpoint out of a staged with single lastStage.
                typeEndpoint = "Single w/State";
                MatsEndpoint<?, ?> ep = matsFactoryToUse.staged(matsMapping.endpointId(), replyType, stoType);
                ep.lastStage(dtoType, (processContext, state, incomingDto) -> {
                    Object reply = invokeMatsMappingMethod(matsMapping, method, bean,
                            paramsLength, processContextParamF,
                            processContext, dtoParamF,
                            incomingDto, stoParamF, state);
                    return helperCast(reply);
                });
            }
            else {
                // -> No state parameter, so use the proper Single endpoint.
                typeEndpoint = "Single";
                matsFactoryToUse.single(matsMapping.endpointId(), replyType, dtoType,
                        (processContext, incomingDto) -> {
                            Object reply = invokeMatsMappingMethod(matsMapping, method, bean,
                                    paramsLength, processContextParamF,
                                    processContext, dtoParamF,
                                    incomingDto, stoParamF, null);
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

    private MatsFactory getMatsFactoryToUse(Method method, Class<? extends Annotation> aeCustomQualifierType,
            String aeQualifierValue, String aeBeanName) {
        List<MatsFactory> specifiedMatsFactories = new ArrayList<>();

        int numberOfQualifications = 0;

        Annotation[] annotations = AnnotationUtils.getAnnotations(method);
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
                    + " Check your specifications on the method [" + method + "].");
        }

        // ?: Did the specific MatsFactory logic end up with more than one MatsFactory?
        if (specifiedMatsFactories.size() > 1) {
            // -> Yes, and that's an ambiguous qualification, which we don't like.
            throw new BeanCreationException("When trying to get specific MatsFactory based on @Mats..-annotation"
                    + " properties; and @Qualifier-annotations and custom qualifier annotations on the"
                    + " @Mats..-annotated method, we ended up with more than one MatsFactory."
                    + " Check your specifications on the method [" + method + "].");
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

    private MatsFactory getMatsFactoryByQualifierValue(String qualifier) {
        // :: Cache lookup
        MatsFactory matsFactory = _cache_MatsFactoryByQualifierValue.get(qualifier);
        if (matsFactory != null) {
            return matsFactory;
        }
        // E-> Didn't find in cache, see if we can find it now.
        try {
            matsFactory = BeanFactoryAnnotationUtils.qualifiedBeanOfType(_configurableListableBeanFactory,
                    MatsFactory.class, qualifier);
            // Cache, and return
            _cache_MatsFactoryByQualifierValue.put(qualifier, matsFactory);
            return matsFactory;
        }
        catch (NoUniqueBeanDefinitionException e) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there was MULTIPLE MatsFactories available in the"
                    + " Spring ApplicationContext with the qualifier '" + qualifier + "', this is probably not"
                    + " what you want.", e);
        }
        catch (NoSuchBeanDefinitionException e) {
            throw new BeanCreationException("When trying to perform Spring-based MATS Endpoint creation, " + this
                    .getClass().getSimpleName() + " found that there is no MatsFactory with the qualifier '" + qualifier
                    + "' available in the Spring ApplicationContext", e);
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
     * Insane cast helper. <i>Please... help. me.</i>.
     */
    @SuppressWarnings("unchecked")
    private static <R> R helperCast(Object reply) {
        return (R) reply;
    }

    /**
     * Helper for invoking the "method ref" @MatsMapping-annotated method that constitute the Mats process-lambda for
     * SingleStage, SingleStage w/ State, and Terminator mappings.
     */
    private static Object invokeMatsMappingMethod(MatsMapping matsMapping, Method method, Object bean,
            int paramsLength, int processContextParamIdx,
            ProcessContext<?> processContext, int dtoParamIdx,
            Object dto, int stoParamIdx, Object sto)
            throws MatsRefuseMessageException {
        Object[] args = new Object[paramsLength];
        args[dtoParamIdx] = processContext;
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
                    + descString(matsMapping, method, bean) + ".", e);
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof MatsRefuseMessageException) {
                throw (MatsRefuseMessageException) e.getTargetException();
            }
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new MatsSpringInvocationTargetException("Got InvocationTargetException when invoking "
                    + descString(matsMapping, method, bean) + ".", e);
        }
    }

    /**
     * Process a method annotated with {@link MatsStaged @MatsStaged} - note that one method can have multiple such
     * annotations, and this method will be invoked for each of them.
     */
    private void processMatsStaged(MatsStaged matsStaged, Method method, Object bean) {
        if (log.isDebugEnabled()) log.debug(LOG_PREFIX + "Processing @MatsStaged [" + matsStaged + "] on method ["
                + method + "], of bean: " + bean);

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
            throw new IllegalStateException("The " + descString(matsStaged, method, bean)
                    + " must have at least one parameter: A MatsEndpoint.");
        }
        else if (paramsLength == 1) {
            // -> One param, must be the MatsEndpoint instances
            if (!params[0].getType().equals(MatsEndpoint.class)) {
                throw new IllegalStateException("The " + descString(matsStaged, method, bean)
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
                throw new IllegalStateException("The " + descString(matsStaged, method, bean)
                        + " consists of several parameters, one of which needs to be the MatsEndpoint.");
            }
        }

        // ----- We've found the parameters.

        // :: Invoke the @MatsStaged-annotated staged endpoint setup method

        MatsFactory matsFactoryToUse = getMatsFactoryToUse(method, matsStaged.matsFactoryCustomQualifierType(),
                matsStaged.matsFactoryQualifierValue(), matsStaged.matsFactoryBeanName());
        MatsEndpoint<?, ?> endpoint = matsFactoryToUse
                .staged(matsStaged.endpointId(), matsStaged.reply(), matsStaged.state());

        // Invoke the @MatsStaged-annotated setup method
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
                    + descString(matsStaged, method, bean) + ".", e);
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new MatsSpringConfigException("Got InvocationTargetException when invoking "
                    + descString(matsStaged, method, bean) + ".", e);
        }

        log.info(LOG_PREFIX + "Processed Staged Mats Spring endpoint by "
                + descString(matsStaged, method, bean) + " :: MatsEndpoint:[param#" + endpointParam
                + "], EndpointConfig:[param#" + endpointParam + "]");
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
     * Thrown if the invocation of a {@link MatsMapping @MatsMapping} or {@link MatsStaged @MatsStaged} annotated method
     * raises {@link InvocationTargetException} and the underlying exception is not a {@link RuntimeException}.
     */
    public static class MatsSpringInvocationTargetException extends RuntimeException {
        public MatsSpringInvocationTargetException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static String descString(Annotation annotation, Method method, Object bean) {
        return "@" + annotation.annotationType().getSimpleName() + "-annotated method '" + bean.getClass()
                .getSimpleName() + "." + method.getName() + "(...)'";
    }
}
