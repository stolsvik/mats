package com.stolsvik.mats;

import java.util.function.Consumer;

import com.stolsvik.mats.MatsConfig.StartStoppable;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.MatsStage.StageConfig;

/**
 * The start point for all interaction with MATS - you need to get hold of an instance of this interface to be able to
 * code MATS endpoints. This is an implementation specific feature (you might want a JMS-specific {@link MatsFactory},
 * backed by a ActiveMQ-specific JMS ConnectionFactory).
 * <p>
 * It is worth realizing that all of the methods {@link #staged(String, Class, Class, Consumer) staged(...config)};
 * {@link #single(String, Class, Class, ProcessSingleLambda) single(...)} and
 * {@link #single(String, Class, Class, Consumer, Consumer, ProcessSingleLambda) single(...configs)};
 * {@link #terminator(String, Class, Class, ProcessTerminatorLambda) terminator(...)} and
 * {@link #terminator(String, Class, Class, Consumer, Consumer, ProcessTerminatorLambda) terminator(...configs)} are
 * just convenience methods to the one {@link #staged(String, Class, Class) staged(...)}. They could just as well have
 * resided in a utility-class. They are included in the API since these relatively few methods seem to cover most
 * scenarios. <i>(Exception to this are the two
 * {@link #subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda) subscriptionTerminator(...)} and
 * {@link #subscriptionTerminator(String, Class, Class, Consumer, Consumer, ProcessTerminatorLambda)
 * subscriptionTerminator(...Consumers)}, as they have different semantics, read the JavaDoc).</i>
 *
 * @author Endre Stølsvik - 2015-07-11 - http://endre.stolsvik.com
 */
public interface MatsFactory extends StartStoppable {

    /**
     * @return the {@link FactoryConfig} on which to configure the factory, e.g. defaults for concurrency.
     */
    FactoryConfig getFactoryConfig();

    /**
     * Simple Consumer&lt;MatsConfig&gt-implementation that does nothing, for use where you e.g. only need to config the
     * stage, not the endpoint, in e.g. the method
     * {@link #terminator(String, Class, Class, Consumer, Consumer, ProcessTerminatorLambda)}.
     */
    Consumer<MatsConfig> NO_CONFIG = config -> {
        /* no-op */ };

    /**
     * Sets up a {@link MatsEndpoint} on which you will add stages. The first stage is the one that will receive the
     * incoming (typically request) DTO, while any subsequent stage is invoked when the service that the previous stage
     * sent a request to, replies.
     * <p>
     * Unless the state object was sent along with the {@link MatsInitiate#request(Object , Object , Object) request} or
     * {@link MatsInitiate#send(Object, Object) send}, the first stage will get a newly constructed empty state
     * instance, while the subsequent stages will get the state instance in the form it was left in the previous stage.
     *
     * @param endpointId
     *            the identification of this {@link MatsEndpoint}, which are the strings that should be provided to the
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String)} methods for this endpoint to
     *            get the message.
     * @param stateClass
     *            the class of the State DTO that will be sent along the stages.
     * @param replyClass
     *            the class that this endpoint shall return.
     * @return the {@link MatsEndpoint} on which to add stages.
     */
    <S, R> MatsEndpoint<S, R> staged(String endpointId, Class<S> stateClass, Class<R> replyClass);

    /**
     * Variation of {@link #staged(String, Class, Class)} that can be configured "on the fly".
     */
    <S, R> MatsEndpoint<S, R> staged(String endpointId, Class<S> stateClass, Class<R> replyClass,
            Consumer<? super EndpointConfig<S, R>> endpointConfigLambda);

    /**
     * Sets up a {@link MatsEndpoint} that just contains one stage, useful for simple
     * "request the full person data for this/these personId(s)" scenarios. This sole stage is supplied directly, using
     * a specialization of the processor lambda which does not have state (as there is only one stage, there is no other
     * stage to pass state to), but which can return the reply by simply returning it on exit from the lambda.
     * <p>
     * Do note that this is just a convenience for the often-used scenario where for example a request will just be
     * looked up in the backing data store, and replied directly, using only one stage, not needing any multi-stage
     * processing.
     *
     * @param endpointId
     *            the identification of this {@link MatsEndpoint}, which are the strings that should be provided to the
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String)} methods for this endpoint to
     *            get the message.
     * @param incomingClass
     *            the class of the incoming (typically request) DTO.
     * @param replyClass
     *            the class that this endpoint shall return.
     * @param processor
     *            the {@link MatsStage stage} that will be invoked to process the incoming message.
     * @return the {@link MatsEndpoint}, but you should not add any stages to it, as the sole stage is already added.
     */
    <I, R> MatsEndpoint<Void, R> single(String endpointId,
            Class<I> incomingClass,
            Class<R> replyClass,
            ProcessSingleLambda<I, R> processor);

    /**
     * Variation of {@link #single(String, Class, Class, ProcessSingleLambda)} that can be configured "on the fly".
     */
    <I, R> MatsEndpoint<Void, R> single(String endpointId,
            Class<I> incomingClass,
            Class<R> replyClass,
            Consumer<? super EndpointConfig<Void, R>> endpointConfigLambda,
            Consumer<? super StageConfig<I, Void, R>> stageConfigLambda,
            ProcessSingleLambda<I, R> processor);

    /**
     * Sets up a {@link MatsEndpoint} that contains a single stage that typically will be the reply-to endpointId for a
     * {@link MatsInitiate#request(Object, Object) request initiation}, or that can be used to directly send a
     * "fire-and-forget" style {@link MatsInitiate#send(Object) invocation} to. The sole stage is supplied directly.
     * This type of endpoint cannot reply, as it has no-one to reply to (hence "terminator").
     * <p>
     * Do note that this is just a convenience for a often-used scenario where a request goes out to some service
     * (possibly recursing down into further services), and then the reply needs to be handled, and then the process is
     * finished. There is nothing hindering you in setting the reply-to endpointId for a request initiation to point to
     * a multi-stage endpoint, and hence have the ability to do further request-replies on the reply from the initial
     * request.
     *
     * @param endpointId
     *            the identification of this {@link MatsEndpoint}, which are the strings that should be provided to the
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String)} methods for this endpoint to
     *            get the message.
     * @param incomingClass
     *            the class of the incoming (typically reply) DTO.
     * @param stateClass
     *            the class of the State DTO that will may be provided by the
     *            {@link MatsInitiate#request(Object, Object) request initiation} (or that was sent along with the
     *            {@link MatsInitiate#send(Object, Object) invocation}).
     * @param processor
     *            the {@link MatsStage stage} that will be invoked to process the incoming message.
     * @return the {@link MatsEndpoint}, but you should not add any stages to it, as the sole stage is already added.
     */
    <I, S> MatsEndpoint<S, Void> terminator(String endpointId,
            Class<I> incomingClass,
            Class<S> stateClass,
            ProcessTerminatorLambda<I, S> processor);

    /**
     * Variation of {@link #terminator(String, Class, Class, ProcessTerminatorLambda)} that can be configured
     * "on the fly".
     */
    <I, S> MatsEndpoint<S, Void> terminator(String endpointId,
            Class<I> incomingClass,
            Class<S> stateClass,
            Consumer<? super EndpointConfig<S, Void>> endpointConfigLambda,
            Consumer<? super StageConfig<I, S, Void>> stageConfigLambda,
            ProcessTerminatorLambda<I, S> processor);

    /**
     * Special kind of terminator that, in JMS-style terms, subscribes to a topic instead of listening to a queue (i.e.
     * "pub-sub"-style messaging). You may only communicate with this type of endpoints by using the
     * {@link MatsInitiate#publish(Object)} methods.
     * <p>
     * <b>Notice that the concurrency of a SubscriptionTerminator is always 1, as it makes no sense to have multiple
     * processors for a subscription - all of the processors would just get an identical copy of each message.</b> If
     * you do need to handle massive amounts of messages, or your work handling is slow, you should instead of handling
     * the work in the processor itself, rather accept the message as fast as possible and send the work out to be
     * processed by some kind of thread pool.
     *
     * @param endpointId
     *            the identification of this {@link MatsEndpoint}, which are the strings that should be provided to the
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String)} methods for this endpoint to
     *            get the message.
     * @param incomingClass
     *            the class of the incoming (typically reply) DTO.
     * @param stateClass
     *            the class of the State DTO that will may be provided by the
     *            {@link MatsInitiate#request(Object, Object) request initiation} (or that was sent along with the
     *            {@link MatsInitiate#send(Object, Object) invocation}).
     * @param processor
     *            the {@link MatsStage stage} that will be invoked to process the incoming message.
     * @return the {@link MatsEndpoint}, but you should not add any stages to it, as the sole stage is already added.
     */
    <I, S> MatsEndpoint<S, Void> subscriptionTerminator(String endpointId, Class<I> incomingClass, Class<S> stateClass,
            ProcessTerminatorLambda<I, S> processor);

    /**
     * Variation of {@link #subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)} that can be
     * configured "on the fly", <b>but notice that the concurrency of a SubscriptionTerminator is always 1</b>.
     */
    <I, S> MatsEndpoint<S, Void> subscriptionTerminator(String endpointId, Class<I> incomingClass, Class<S> stateClass,
            Consumer<? super EndpointConfig<S, Void>> endpointConfigLambda,
            Consumer<? super StageConfig<I, S, Void>> stageConfigLambda,
            ProcessTerminatorLambda<I, S> processor);

    /**
     * The way to start a MATS process: Get hold of a {@link MatsInitiator}, and fire off messages!
     * <p>
     * <b>Notice: You are <em>not</em> supposed to get one instance of {@link MatsInitiator} per message you need to
     * send - rather either just one for the entire application, or for each component:</b> The {@code MatsInitiator}
     * will have an underlying backend connection attached to it - which also means that it needs to be closed for a
     * clean application shutdown.
     *
     * @param initiatorId
     *            a fictive "endpointId" representing the "initiating endpoint".
     * @return a {@link MatsInitiator}, on which messages can be {@link MatsInitiator#initiate(InitiateLambda)
     *         initiated}.
     */
    MatsInitiator getInitiator(String initiatorId);

    /**
     * "Releases" any start-waiting endpoints, read up on {@link #stop}. Unless {@link #stop()} has been invoked first,
     * this method has no effect.
     */
    @Override
    void start();

    /**
     * Stops the factory, which will invoke {@link MatsEndpoint#stop()} on all the endpoints, and
     * {@link MatsInitiator#close()} on all initiators, that has been created by it. Meant for application shutdown.
     * <p>
     * <b>However, if this is invoked before any endpoint is created, the endpoints will not start even though
     * {@link MatsEndpoint#start()} is invoked on them, but will wait till {@link #start()} is invoked on the
     * factory.</b> This feature should be employed in most setups where the MATS endpoints might use other services
     * whose order of creation and initialization are difficult to fully control, e.g. typically in an IoC container
     * like Spring: Set all stuff up, where order is not of importance, and <i>then</i> fire up the endpoints. If this
     * is not done, the endpoints might start consuming messages off of the MQ (there might already be messages waiting
     * when the service boots), and thus invoke other services that are not yet fully up.
     */
    @Override
    void stop();

    /**
     * Provides for a way to configure factory-wide elements and defaults.
     */
    interface FactoryConfig extends MatsConfig {
        @Override
        FactoryConfig setConcurrency(int concurrency);

        /**
         * @return the suggested key on which to store the {@link MatsTrace} if the underlying mechanism uses a Map,
         *         e.g. a {@code MapMessage} of JMS. Defaults to <code>"mats:trace"</code>.
         */
        default String getMatsTraceKey() {
            return "mats:trace";
        }

        /**
         * @return the suggested prefix for the messaging system's queues and topics that MATS uses. Defaults to
         *         <code>"mats:"</code>.
         */
        default String getMatsDestinationPrefix() {
            return "mats:";
        }
    }
}
