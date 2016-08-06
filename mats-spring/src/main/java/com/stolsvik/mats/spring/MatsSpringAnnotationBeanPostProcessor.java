package com.stolsvik.mats.spring;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Map;
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
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsFactory;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;
import com.stolsvik.mats.exceptions.MatsRuntimeException;
import com.stolsvik.mats.spring.MatsMapping.MatsMappings;

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
            Map<Method, Set<MatsMapping>> matsEndpointMethods = MethodIntrospector.selectMethods(targetClass,
                    (MetadataLookup<Set<MatsMapping>>) method -> {
                        Set<MatsMapping> matsEndpoints = AnnotationUtils.getRepeatableAnnotations(method,
                                MatsMapping.class, MatsMappings.class);
                        return (!matsEndpoints.isEmpty() ? matsEndpoints : null);
                    });
            // ?: Are there any annotated methods on this class?
            if (matsEndpointMethods.isEmpty()) {
                // -> No, so cache that fact, to short-circuit discovery next time for this class (e.g. prototype beans)
                _classesWithNoMatsMappingAnnotations.add(targetClass);
                if (log.isTraceEnabled()) {
                    log.trace(LOG_PREFIX + "No @MatsMapping annotations found on bean class [" + bean.getClass()
                            + "].");
                }
            }
            else {
                // Yes, there was annotations. Process them.
                for (Map.Entry<Method, Set<MatsMapping>> entry : matsEndpointMethods.entrySet()) {
                    Method method = entry.getKey();
                    for (MatsMapping matsEndpoint : entry.getValue()) {
                        processMatsMapping(matsEndpoint, method, bean);
                    }
                }
            }
        }
        return bean;
    }

    private void processMatsMapping(MatsMapping matsMapping, Method method, Object bean) {
        if (log.isTraceEnabled()) {
            log.trace(LOG_PREFIX + "Processing @MatsMapping [" + matsMapping + "] on method [" + method
                    + "], of bean: " + bean);
        }

        method.setAccessible(true);

        // :: Assert that the endpoint is not annotated with @Transactional
        // (Should probably also check whether it has gotten transactional AOP by XML, but who is stupid enough
        // to use XML config in 2016?!)
        Transactional transactionalAnnotation = AnnotationUtils.findAnnotation(method, Transactional.class);
        if (transactionalAnnotation != null) {
            throw new IllegalStateException("The " + descString(method, bean)
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
        if (paramsLength == 0) {
            throw new IllegalStateException("The " + descString(method, bean)
                    + " must have at least one parameter: The DTO class.");
        }
        else if (paramsLength == 1) {
            if (params[0].getType().equals(ProcessContext.class)) {
                throw new IllegalStateException("The " + descString(method, bean)
                        + " must have one parameter that is not the"
                        + " ProcessContext: The DTO class");
            }
            dtoParam = 0;
        }
        else {
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
                dtoParam = processContextParam ^ 1;
            }
            else {
                // -> Either ProcessContext + more-than-one extra, or no ProcessContext and more-than-one param.
                // Find DTO and STO
                for (int i = 0; i < paramsLength; i++) {
                    if (params[i].getAnnotation(Dto.class) != null) {
                        dtoParam = i;
                    }
                    if (params[i].getAnnotation(Sto.class) != null) {
                        stoParam = i;
                    }
                }
                if (dtoParam == -1) {
                    throw new IllegalStateException("The " + descString(method, bean)
                            + " consists of several parameters, one of which needs to be annotated with @Dto");
                }
            }
        }

        Class<?> dtoType = params[dtoParam].getType();
        Class<?> stoType = (stoParam == -1 ? Void.class : params[stoParam].getType());

        final int dtoParamF = dtoParam;
        final int processContextParamF = processContextParam;
        final int stoParamF = stoParam;

        final Class<?> replyType = method.getReturnType();

        String typeEndpoint;

        if (replyType.getName().equals("void")) {
            // -> No reply, setup Terminator.
            typeEndpoint = "Terminator";
            matsFactory.terminator(matsMapping.endpointId(), dtoType, stoType,
                    (processContext, incomingDto, state) -> {
                        invokeMatsMapping(method, bean, processContext,
                                paramsLength, processContextParamF,
                                dtoParamF, incomingDto,
                                stoParamF, state);
                    });
        }
        else {
            // -> A ReplyType is provided, setup Single endpoint.
            // ?: Is a State parameter specified?
            if (stoParamF != -1) {
                // -> Yes, so then we need to hack together a "single" endpoint out of a staged with single lastStage.
                typeEndpoint = "Single w/State";
                MatsEndpoint<?, ?> ep = matsFactory.staged(matsMapping.endpointId(), stoType, replyType);
                ep.lastStage(dtoType, (processContext, incomingDto, state) -> {
                    Object reply = invokeMatsMapping(method, bean, processContext,
                            paramsLength, processContextParamF,
                            dtoParamF, incomingDto,
                            stoParamF, state);
                    return helperCast(reply);
                });
            }
            else {
                // -> No, so use the proper Single endpoint.
                typeEndpoint = "Single";
                matsFactory.single(matsMapping.endpointId(), dtoType, replyType,
                        (processContext, incomingDto) -> {
                            Object reply = invokeMatsMapping(method, bean, processContext,
                                    paramsLength, processContextParamF,
                                    dtoParamF, incomingDto,
                                    stoParamF, null);
                            return helperCast(reply);
                        });
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(LOG_PREFIX + "@MatsMapping of [" + typeEndpoint + "]-Endpoint on method ["
                    + bean.getClass().getName() + "." + method.getName() + "(..)]"
                    + " :: ReplyType:" + replyType
                    + ", ProcessContext:#" + processContextParam
                    + ", DTO:#" + dtoParam + ":" + dtoType.getSimpleName()
                    + ", STO:#" + stoParam + ":" + stoType.getSimpleName());
        }
    }

    /**
     * Insane cast helper. Please help me.
     */
    @SuppressWarnings("unchecked")
    private static <R> R helperCast(Object reply) {
        return (R) reply;
    }

    private static Object invokeMatsMapping(Method method, Object bean, ProcessContext<?> processContext,
            int paramsNo, int processContextParamF,
            int dtoParam, Object dto,
            int stoParam, Object sto)
                    throws MatsRefuseMessageException {
        Object[] args = new Object[paramsNo];
        args[dtoParam] = processContext;
        if (processContextParamF != -1) {
            args[processContextParamF] = processContext;
        }
        if (dtoParam != -1) {
            args[dtoParam] = dto;
        }
        if (stoParam != -1) {
            args[stoParam] = sto;
        }
        try {
            return method.invoke(bean, args);
        }
        catch (IllegalAccessException | IllegalArgumentException e) {
            throw new MatsRefuseMessageException("Problem with invoking @MatsMapping", e);
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof RuntimeException) {
                throw (RuntimeException) e.getTargetException();
            }
            throw new MatsRuntimeException("Got InvocationTargetException when invoking @MatsMapping",
                    e);
        }
    }

    private String descString(Method method, Object bean) {
        return "@MatsMapping-annotated method '" + bean.getClass().getSimpleName() + "." + method.getName() + "(...)";
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
