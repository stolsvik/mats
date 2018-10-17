package com.stolsvik.mats.spring;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.MethodIntrospector.MetadataLookup;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.transaction.annotation.Transactional;

import com.stolsvik.mats.MatsEndpoint;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.MatsEndpoint.MatsRefuseMessageException;
import com.stolsvik.mats.exceptions.MatsRuntimeException;
import com.stolsvik.mats.spring.MatsMapping.MatsMappings;
import com.stolsvik.mats.spring.MatsStaged.MatsStageds;

/**
 * @author Endre Stølsvik - 2016-05-21 - http://endre.stolsvik.com
 */
public class MatsSpringAnnotationBeanPostProcessor implements
        BeanPostProcessor,
        Ordered,
        ApplicationContextAware,
        ApplicationListener<ApplicationContextEvent>,
        SmartLifecycle {

    public static final String LOG_PREFIX = "#SPRINGMATS# ";

    // Use clogging, since that's what Spring does.
    private static final Log log = LogFactory.getLog(MatsSpringAnnotationBeanPostProcessor.class);

    /**
     * The {@literal @Configuration}-class specified by the {@link EnableMats @EnableMats} annotation.
     */
    @Configuration
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public static class MatsSpringConfiguration {
        @Bean
        @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
        public MatsSpringAnnotationBeanPostProcessor matsSpringAnnotationBeanPostProcessor() {
            return new MatsSpringAnnotationBeanPostProcessor();
        }
    }

    @Inject
    private MatsFactory matsFactory;

    @Override
    public int getOrder() {
        log.info(LOG_PREFIX + "Ordered.getOrder()");
        if (true) {
            throw new AssertionError("I thought Ordered.getOrder() was not invoked. How come it suddenly is?");
        }
        return LOWEST_PRECEDENCE - 1;
    }

    private ApplicationContext _applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        log.info(LOG_PREFIX + "ApplicationContextAware.setApplicationContext('" + applicationContext.getClass()
                .getSimpleName()
                + "'): " + applicationContext);
        _applicationContext = applicationContext;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        log.info(LOG_PREFIX + "BeanPostProcessor.postProcessBeforeInitialization('" + beanName + "'): " + bean);
        return bean;
    }

    private final Set<Class<?>> _classesWithNoMatsMappingAnnotations = ConcurrentHashMap.newKeySet();

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        log.info(LOG_PREFIX + "BeanPostProcessor.postProcessAfterInitialization('" + beanName + "'): " + bean);

        Class<?> targetClass = AopUtils.getTargetClass(bean);
        if (!_classesWithNoMatsMappingAnnotations.contains(targetClass)) {
            Map<Method, Set<MatsMapping>> methodsWithMatsMappingAnnotations = findAnnotatedMethods(targetClass,
                    MatsMapping.class, MatsMappings.class);
            Map<Method, Set<MatsStaged>> methodsWithMatsStagedAnnotations = findAnnotatedMethods(targetClass,
                    MatsStaged.class, MatsStageds.class);

            // ?: Are there any Mats annotated methods on this class?
            if (methodsWithMatsMappingAnnotations.isEmpty() && methodsWithMatsStagedAnnotations.isEmpty()) {
                // -> No, so cache that fact, to short-circuit discovery next time for this class (e.g. prototype beans)
                _classesWithNoMatsMappingAnnotations.add(targetClass);
                if (log.isTraceEnabled()) {
                    log.trace(LOG_PREFIX + "No @MatsMapping or @MatsStaged annotations found on bean class ["
                            + bean.getClass() + "].");
                }
                return bean;
            }
            // ?: Any @MatsMapping annotated methods?
            if (!methodsWithMatsMappingAnnotations.isEmpty()) {
                // -> Yes, there are @MatsMapping annotations. Process them.
                for (Entry<Method, Set<MatsMapping>> entry : methodsWithMatsMappingAnnotations.entrySet()) {
                    Method method = entry.getKey();
                    for (MatsMapping matsMapping : entry.getValue()) {
                        processMatsMapping(matsMapping, method, bean);
                    }
                }
            }
            // ?: Any @MatsStaged annotated methods?
            if (!methodsWithMatsStagedAnnotations.isEmpty()) {
                // -> Yes, there are @MatsStaged annotations. Process them.
                for (Entry<Method, Set<MatsStaged>> entry : methodsWithMatsStagedAnnotations.entrySet()) {
                    Method method = entry.getKey();
                    for (MatsStaged matsMapping : entry.getValue()) {
                        processMatsStaged(matsMapping, method, bean);
                    }
                }
            }
        }
        return bean;
    }

    private <A extends Annotation> Map<Method, Set<A>> findAnnotatedMethods(Class<?> targetClass,
            Class<A> single, Class<? extends Annotation> plural) {
        return MethodIntrospector.selectMethods(targetClass, (MetadataLookup<Set<A>>) method -> {
            Set<A> matsMappingAnnotations = AnnotationUtils.getRepeatableAnnotations(method, single, plural);
            return (!matsMappingAnnotations.isEmpty() ? matsMappingAnnotations : null);
        });
    }

    /**
     * Processes methods annotated with {@link MatsStaged @MatsMapping}.
     */
    private void processMatsMapping(MatsMapping matsMapping, Method method, Object bean) {
        if (log.isDebugEnabled()) {
            log.debug(LOG_PREFIX + "Processing @MatsMapping [" + matsMapping + "] on method ["
                    + method + "], of bean: " + bean);
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
            matsFactory.terminator(matsMapping.endpointId(), stoType, dtoType,
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
                MatsEndpoint<?, ?> ep = matsFactory.staged(matsMapping.endpointId(), replyType, stoType);
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
                matsFactory.single(matsMapping.endpointId(), replyType, dtoType,
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
            log.info(LOG_PREFIX + "Processed " + typeEndpoint + " Mats Spring endpoint by "
                    + descString(matsMapping, method, bean) + " :: ReplyType:[" + replyType
                    + "], ProcessContext:[param#" + processContextParam
                    + "], STO:[param#" + stoParam + ":" + stoType.getSimpleName()
                    + "], DTO:[param#" + dtoParam + ":" + dtoType.getSimpleName() + "]");
        }
    }

    /**
     * Insane cast helper. Please help me.
     */
    @SuppressWarnings("unchecked")
    private static <R> R helperCast(Object reply) {
        return (R) reply;
    }

    /**
     * Thrown if the setup of a Mats Spring endpoint fails.
     */
    public static class MatsSpringConfigException extends MatsRuntimeException {
        public MatsSpringConfigException(String message, Throwable cause) {
            super(message, cause);
        }

        public MatsSpringConfigException(String message) {
            super(message);
        }
    }

    /**
     * Helper for invoking the "method ref" @MatsMapping-annotated method that constitute the Mats process-lambda for
     * SingleStage, SingleStage w/ State, and Terminator mappings.
     *
     * @param matsMapping
     *            TODO
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
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new MatsSpringInvocationTargetException("Got InvocationTargetException when invoking "
                    + descString(matsMapping, method, bean) + ".", e);
        }
    }

    /**
     * Thrown if the invocation of a {@link MatsMapping @MatsMapping} or {@link MatsStaged @MatsStaged} annotated method
     * raises {@link InvocationTargetException} and the underlying exception is not a {@link RuntimeException}.
     */
    public static class MatsSpringInvocationTargetException extends MatsRuntimeException {
        public MatsSpringInvocationTargetException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Processes methods annotated with {@link MatsStaged @MatsStaged}.
     */
    private void processMatsStaged(MatsStaged matsStaged, Method method, Object bean) {
        if (log.isDebugEnabled()) {
            log.debug(LOG_PREFIX + "Processing @MatsStaged [" + matsStaged + "] on method ["
                    + method + "], of bean: " + bean);
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

        MatsEndpoint<?, ?> endpoint = matsFactory.staged(matsStaged.endpointId(), matsStaged
                .reply(), matsStaged.state());

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
            throw new MatsSpringInvocationTargetException("Got InvocationTargetException when invoking "
                    + descString(matsStaged, method, bean) + ".", e);
        }

        if (log.isInfoEnabled()) {
            log.info(LOG_PREFIX + "Processed Staged Mats Spring endpoint by "
                    + descString(matsStaged, method, bean) + " :: MatsEndpoint:[param#" + endpointParam
                    + "], EndpointConfig:[param#" + endpointParam + "]");
        }
    }

    private static String descString(Annotation annotation, Method method, Object bean) {
        return "@" + annotation.getClass().getSimpleName() + "-annotated method '" + bean.getClass().getSimpleName()
                + "#" + method.getName() + "(...)'";
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        log.info(LOG_PREFIX + "ApplicationListener.onApplicationEvent('" + event.getClass().getSimpleName() + "'): "
                + event);
    }

    @Override
    public void start() {
        log.info(LOG_PREFIX + "Lifecycle.start()");
        _running = true;
    }

    @Override
    public void stop() {
        log.info(LOG_PREFIX + "Lifecycle.stop()");
    }

    private boolean _running = false;

    @Override
    public boolean isRunning() {
        log.info(LOG_PREFIX + "Lifecycle.isRunning()");
        return _running;
    }

    @Override
    public int getPhase() {
        log.info(LOG_PREFIX + "Phased.getPhase()");
        return 0;
    }

    @Override
    public boolean isAutoStartup() {
        log.info(LOG_PREFIX + "SmartLifecycle.isAutoStartup()");
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        log.info(LOG_PREFIX + "SmartLifecycle.stop()");
        _running = false;
        callback.run();
    }
}
