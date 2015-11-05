package com.stolsvik.mats;

import com.stolsvik.mats.MatsConfig.ConfigLambda;
import com.stolsvik.mats.MatsConfig.StartClosable;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;

/**
 * The start point for all interaction with MATS - you need to get hold of an instance of this interface to be able to
 * code MATS endpoints, this is an implementation specific feature (you might want a JMS-specific {@link MatsFactory},
 * backed by a ActiveMQ-specific JMS ConnectionFactory).
 *
 * @author Endre Stølsvik - 2015-07-11 - http://endre.stolsvik.com
 */
public interface MatsFactory extends StartClosable {

    /**
     * @return the {@link FactoryConfig} on which to configure the factory, e.g. defaults for concurrency.
     */
    FactoryConfig getFactoryConfig();

    /**
     * Sets up a {@link MatsEndpoint} on which you will add stages. The first stage is the one that will receive the
     * incoming (typically request) DTO, while any subsequent stage is invoked when the service that the previous stage
     * sent a request to, replies.
     * <p>
     * Unless the state object was sent along with request or invocation, the first stage will get a newly constructed
     * "empty" state instance, while the subsequent stages will get the state instance in the form it was left in the
     * previous stage.
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
            ConfigLambda<EndpointConfig> configLambda);

    /**
     * Sets up a {@link MatsEndpoint} that just contains one stage, useful for simple
     * "request the full person data for this persionId" scenarios. This sole stage is supplied directly, using a
     * specialization of the processor lambda which does not have state (as there is only one stage, there is no other
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
    <I, R> MatsEndpoint<Void, R> single(String endpointId, Class<I> incomingClass, Class<R> replyClass,
            ProcessSingleLambda<I, R> processor);

    /**
     * Variation of {@link #single(String, Class, Class, ProcessSingleLambda)} that can be configured "on the fly".
     */
    <I, R> MatsEndpoint<Void, R> single(String endpointId, Class<I> incomingClass, Class<R> replyClass,
            ConfigLambda<EndpointConfig> configLambda, ProcessSingleLambda<I, R> processor);

    /**
     * Sets up a {@link MatsEndpoint} that contains a single stage that typically will be the reply-to endpointId for a
     * {@link MatsInitiate#request(Object, Object) request initiation}, or that can be used to send a "fire-and-forget"
     * style {@link MatsInitiate#send(Object) invocation} to. The sole stage is supplied directly. This type of endpoint
     * cannot reply, as it has no-one to reply to (hence "terminator").
     * <p>
     * Do note that this is just a convenience for a often-used scenario where a request goes out to some service, and
     * the reply needs to be handled, and then one is finished. There is nothing hindering you in setting the reply-to
     * endpointId to a multi-stage endpoint, and hence have the ability to do further request-replies on the reply from
     * the initial request.
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
    <I, S> MatsEndpoint<S, Void> terminator(String endpointId, Class<I> incomingClass, Class<S> stateClass,
            ProcessTerminatorLambda<I, S> processor);

    /**
     * Variation of {@link #terminator(String, Class, Class, ProcessTerminatorLambda)} that can be configured
     * "on the fly".
     */
    <I, S> MatsEndpoint<S, Void> terminator(String endpointId, Class<I> incomingClass, Class<S> stateClass,
            ConfigLambda<EndpointConfig> configLambda, ProcessTerminatorLambda<I, S> processor);

    /**
     * Special kind of terminator that, in JMS-style terms, subscribes to a topic instead of listening to a queue. You
     * may only communicate with this type of endpoints by using the {@link MatsInitiate#publish(Object)} methods.
     * <p>
     * <b>Notice that the concurrency of a SubscriptionTerminator is always 1</b>.
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
     * configured "on the fly", but <b>notice that the concurrency of a SubscriptionTerminator is always 1</b>.
     */
    <I, S> MatsEndpoint<S, Void> subscriptionTerminator(String endpointId, Class<I> incomingClass, Class<S> stateClass,
            ConfigLambda<EndpointConfig> configLambda, ProcessTerminatorLambda<I, S> processor);

    /**
     * The way to start a MATS process: Get hold of a {@link MatsInitiator}, and fire off messages!
     * <p>
     * <b>Notice: You are not supposed to get one per sent message, rather either just one for the entire application,
     * or for each component:</b> It will have an underlying backend connection attached to it, and should hence also be
     * closed for a clean application shutdown.
     *
     * @param initiatorId
     *            a fictive "endpointId" representing the "initiating endpoint".
     * @return a {@link MatsInitiator}, on which messages can be {@link MatsInitiator#initiate(InitiateLambda)
     *         initiated}.
     */
    MatsInitiator getInitiator(String initiatorId);

    /**
     * "Releases" any start-waiting endpoints, read up on {@link #close}. Unless {@link #close()} has been invoked
     * first, this method has no effect.
     */
    @Override
    void start();

    /**
     * Stops the factory, which will invoke {@link MatsEndpoint#close()} on all the endpoints, and
     * {@link MatsInitiator#close()} on all initiators, that has been created by it. Meant for application shutdown.
     * <p>
     * <b>However, if this is invoked before any endpoint is created, the endpoints will not start even though
     * {@link MatsEndpoint#start()} is invoked on them, but will wait till {@link #start()} is invoked on the
     * factory.</b> This can be useful to halt endpoint startup until the entire application is finished configured, so
     * that one does not end in a situation where an endpoint receives a message and starts processing it, employing
     * services that have not yet finished configuration. <b>Either this functionality, or the
     * {@link FactoryConfig#setStartDelay(int)} should probably be employed for any application.</b>
     */
    @Override
    void close();

    /**
     * Provides for a way to configure factory-wide elements and defaults.
     */
    interface FactoryConfig extends MatsConfig {
        @Override
        FactoryConfig setConcurrency(int concurrency);

        /**
         * Sets the start delay, which may be necessary if there are startup asynchronicity that may prevent the
         * endpoints from working properly if they are started right away, e.g. some application service has not yet
         * started. Defaults to 0 ms, while 2500 ms should be enough for anyone.
         * <p>
         * One should probably either use this functionality, or make sure the {@link MatsFactory} is constructed in a
         * {@link MatsFactory#close() stopped condition}, only starting it when the entire application is finished with
         * configuration.
         *
         * @param milliseconds
         *            the number of milliseconds delay after start is invoked for each process stage.
         * @return the {@link FactoryConfig} for method chaining.
         */
        FactoryConfig setStartDelay(int milliseconds);

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
