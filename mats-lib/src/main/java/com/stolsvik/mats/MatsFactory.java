package com.stolsvik.mats;

import java.util.function.Consumer;

import com.stolsvik.mats.MatsConfig.StartStoppable;
import com.stolsvik.mats.MatsEndpoint.EndpointConfig;
import com.stolsvik.mats.MatsEndpoint.ProcessContext;
import com.stolsvik.mats.MatsEndpoint.ProcessLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessSingleLambda;
import com.stolsvik.mats.MatsEndpoint.ProcessTerminatorLambda;
import com.stolsvik.mats.MatsInitiator.InitiateLambda;
import com.stolsvik.mats.MatsInitiator.MatsInitiate;
import com.stolsvik.mats.MatsStage.StageConfig;

/**
 * The start point for all interaction with MATS - you need to get hold of an instance of this interface to be able to
 * code MATS endpoints, and to perform initiations (i.e. send a message, perform a request, publish a message). This is
 * an implementation specific feature (you might want a JMS-specific {@link MatsFactory}, backed by a ActiveMQ-specific
 * JMS ConnectionFactory).
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
 * <p>
 * Regarding order of the Reply Message, State and Incoming Message, which can be a bit annoying to remember when
 * creating endpoints, and when writing {@link ProcessLambda process lambdas}: They are always ordered like this: <b>R,
 * S, I</b>, i.e. <i>Reply, State, Incoming</i>. This is to resemble a method signature having the implicit {@code this}
 * (or {@code self}) reference as the first argument: {@code ReturnType methodName(this, arguments)}. Thus, if you
 * remember that Mats is created to enable you to write messaging oriented endpoints that <i>look like</i> they are
 * methods, then it might stick! The process lambda thus has args (context, state, incomingMsg), unless it lacks state.
 * Even if it lacks state, the context is always first, and the incoming message is always last.<br/>
 * Examples:
 * <ul>
 * <li>In a Terminator, you have an incoming message, and state (which the initiator set) - a terminator doesn't reply.
 * The params of the {@link #terminator(String, Class, Class, ProcessTerminatorLambda) terminator}-method of MatsFactory
 * is thus [EndpointId, State Class, Incoming Class, process lambda]. The lambda params of the terminator will be:
 * [Context, State, Incoming]</li>
 * <li>For a SingleStage endpoint, you will have incoming message, and reply message - there is no state, since that is
 * an object that traverses between the stages in a multi-stage endpoint, and this endpoint is just a single stage. The
 * params of the {@link #single(String, Class, Class, ProcessSingleLambda)} single stage}-method of MatsFactory is thus
 * [EndpointId, Reply Class, Incoming Class, process lambda]. The lambda params will then be: [Context, Incoming] - the
 * reply type is the the return type of the process lambda.</li>
 * </ul>
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
     * Unless the state object was sent along with the {@link MatsInitiate#request(Object, Object) request} or
     * {@link MatsInitiate#send(Object, Object) send}, the first stage will get a newly constructed empty state
     * instance, while the subsequent stages will get the state instance in the form it was left in the previous stage.
     *
     * @param endpointId
     *            the identification of this {@link MatsEndpoint}, which are the strings that should be provided to the
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String, Object)} methods for this
     *            endpoint to get the message. Typical structure is <code>"OrderService.placeOrder"</code> for public
     *            endpoints, or <code>"OrderService.private.validateOrder"</code> for private (app-internal) endpoints.
     * @param replyClass
     *            the class that this endpoint shall return.
     * @param stateClass
     *            the class of the State DTO that will be sent along the stages.
     * @return the {@link MatsEndpoint} on which to add stages.
     */
    <R, S> MatsEndpoint<R, S> staged(String endpointId, Class<R> replyClass, Class<S> stateClass);

    /**
     * Variation of {@link #staged(String, Class, Class)} that can be configured "on the fly".
     */
    <R, S> MatsEndpoint<R, S> staged(String endpointId, Class<R> replyClass, Class<S> stateClass,
            Consumer<? super EndpointConfig<R, S>> endpointConfigLambda);

    /**
     * Sets up a {@link MatsEndpoint} that just contains one stage, useful for simple "request the full person data for
     * this/these personId(s)" scenarios. This sole stage is supplied directly, using a specialization of the processor
     * lambda which does not have state (as there is only one stage, there is no other stage to pass state to), but
     * which can return the reply by simply returning it on exit from the lambda.
     * <p>
     * Do note that this is just a convenience for the often-used scenario where for example a request will just be
     * looked up in the backing data store, and replied directly, using only one stage, not needing any multi-stage
     * processing.
     *
     * @param endpointId
     *            the identification of this {@link MatsEndpoint}, which are the strings that should be provided to the
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String, Object)} methods for this
     *            endpoint to get the message. Typical structure is <code>"OrderService.placeOrder"</code> for public
     *            endpoints, or <code>"OrderService.private.validateOrder"</code> for private (app-internal) endpoints.
     * @param replyClass
     *            the class that this endpoint shall return.
     * @param incomingClass
     *            the class of the incoming (typically request) DTO.
     * @param processor
     *            the {@link MatsStage stage} that will be invoked to process the incoming message.
     * @return the {@link MatsEndpoint}, but you should not add any stages to it, as the sole stage is already added.
     */
    <R, I> MatsEndpoint<R, Void> single(String endpointId,
            Class<R> replyClass,
            Class<I> incomingClass,
            ProcessSingleLambda<R, I> processor);

    /**
     * Variation of {@link #single(String, Class, Class, ProcessSingleLambda)} that can be configured "on the fly".
     */
    <R, I> MatsEndpoint<R, Void> single(String endpointId,
            Class<R> replyClass,
            Class<I> incomingClass,
            Consumer<? super EndpointConfig<R, Void>> endpointConfigLambda,
            Consumer<? super StageConfig<R, Void, I>> stageConfigLambda,
            ProcessSingleLambda<R, I> processor);

    /**
     * Sets up a {@link MatsEndpoint} that contains a single stage that typically will be the reply-to endpointId for a
     * {@link MatsInitiate#request(Object, Object) request initiation}, or that can be used to directly send a
     * "fire-and-forget" style {@link MatsInitiate#send(Object) invocation} to. The sole stage is supplied directly.
     * This type of endpoint cannot reply, as it has no-one to reply to (hence "terminator").
     * <p>
     * Do note that this is just a convenience for the often-used scenario where an initiation requests out to some
     * service, and then the reply needs to be handled - and with that the process is finished. That last endpoint which
     * handles the reply is what is referred to as a terminator, in that it has nowhere to reply to. Note that there is
     * nothing hindering you in setting the replyTo endpointId in a request initiation to point to a single-stage or
     * multi-stage endpoint - however, any replies from those endpoints will just go void.
     * <p>
     * It is possible to {@link ProcessContext#initiate(InitiateLambda) initiate} from within a terminator, and one
     * interesting scenario here is to do a {@link MatsInitiate#publish(Object) publish} to a
     * {@link #subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda) subscriptionTerminator}. The idea
     * is then that you do the actual processing via a request, and upon the reply processing in the terminator, you
     * update the app database with the updated information (e.g. "order is processed"), and then you publish an "update
     * caches" message to all the nodes of the app, so that they all have the new state of the order in their caches
     * (or, in a push-based GUI logic, you might want to update all users' view of that order). Note that you (as in the
     * processing node) will also get that published message on your instance of the SubscriptionTerminator.
     * <p>
     * It is technically possible {@link ProcessContext#reply(Object) reply} from within a terminator - but it hard to
     * envision many wise usage scenarios for this, as the stack at a terminator would probably be empty.
     *
     * @param endpointId
     *            the identification of this {@link MatsEndpoint}, which are the strings that should be provided to the
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String, Object)} methods for this
     *            endpoint to get the message. Typical structure is <code>"OrderService.placeOrder"</code> for public
     *            endpoints (which then is of a "fire-and-forget" style, since a terminator is not meant to reply), or
     *            <code>"OrderService.terminator.validateOrder"</code> for private (app-internal) terminators that is
     *            targeted by the {@link MatsInitiate#replyTo(String, Object) replyTo(endpointId,..)} invocation of an
     *            initiation.
     * @param stateClass
     *            the class of the State DTO that will may be provided by the
     *            {@link MatsInitiate#request(Object, Object) request initiation} (or that was sent along with the
     *            {@link MatsInitiate#send(Object, Object) invocation}).
     * @param incomingClass
     *            the class of the incoming (typically reply) DTO.
     * @param processor
     *            the {@link MatsStage stage} that will be invoked to process the incoming message.
     * @return the {@link MatsEndpoint}, but you should not add any stages to it, as the sole stage is already added.
     */
    <S, I> MatsEndpoint<Void, S> terminator(String endpointId,
            Class<S> stateClass,
            Class<I> incomingClass,
            ProcessTerminatorLambda<S, I> processor);

    /**
     * Variation of {@link #terminator(String, Class, Class, ProcessTerminatorLambda)} that can be configured "on the
     * fly".
     */
    <S, I> MatsEndpoint<Void, S> terminator(String endpointId,
            Class<S> stateClass,
            Class<I> incomingClass,
            Consumer<? super EndpointConfig<Void, S>> endpointConfigLambda,
            Consumer<? super StageConfig<Void, S, I>> stageConfigLambda,
            ProcessTerminatorLambda<S, I> processor);

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
     *            {@link MatsInitiate#to(String)} or {@link MatsInitiate#replyTo(String, Object)} methods for this
     *            endpoint to get the message.
     * @param stateClass
     *            the class of the State DTO that will may be provided by the
     *            {@link MatsInitiate#request(Object, Object) request initiation} (or that was sent along with the
     *            {@link MatsInitiate#send(Object, Object) invocation}).
     * @param incomingClass
     *            the class of the incoming (typically reply) DTO.
     * @param processor
     *            the {@link MatsStage stage} that will be invoked to process the incoming message.
     * @return the {@link MatsEndpoint}, but you should not add any stages to it, as the sole stage is already added.
     */
    <S, I> MatsEndpoint<Void, S> subscriptionTerminator(String endpointId,
            Class<S> stateClass,
            Class<I> incomingClass,
            ProcessTerminatorLambda<S, I> processor);

    /**
     * Variation of {@link #subscriptionTerminator(String, Class, Class, ProcessTerminatorLambda)} that can be
     * configured "on the fly", <b>but notice that the concurrency of a SubscriptionTerminator is always 1</b>.
     */
    <S, I> MatsEndpoint<Void, S> subscriptionTerminator(String endpointId,
            Class<S> stateClass,
            Class<I> incomingClass,
            Consumer<? super EndpointConfig<Void, S>> endpointConfigLambda,
            Consumer<? super StageConfig<Void, S, I>> stageConfigLambda,
            ProcessTerminatorLambda<S, I> processor);

    /**
     * Creates a new Initiator from which to initiate new Mats processes, i.e. send a message from "outside of Mats" to
     * a Mats endpoint - <b>NOTICE: This is an active object that can carry backend resources, and it is Thread Safe,
     * therefore you are not supposed to create one instance per message you send!</b>
     * <p>
     * <b>Observe: The returned MatsInitiator is Thread Safe, and meant for reuse: You are <em>not</em> supposed to
     * create one instance of {@link MatsInitiator} per message you need to send - rather either create one for the
     * entire application, or e.g. for each component:</b> The {@code MatsInitiator} can have underlying backend
     * resources attached to it - which also means that it needs to be {@link MatsInitiator#close() closed} for a clean
     * application shutdown (Note that all MatsInitiators are closed when {@link #stop() MatsFactory.stop()} is
     * invoked).
     *
     * @return a {@link MatsInitiator}, on which messages can be {@link MatsInitiator#initiate(InitiateLambda)
     *         initiated}.
     */
    MatsInitiator createInitiator();

    /**
     * Starts all endpoints that has been created by this factory, by invoking {@link MatsEndpoint#start()} on them.
     * <p>
     * Subsequently clears the {@link #holdEndpointsUntilFactoryIsStarted()}-flag.
     */
    @Override
    void start();

    /**
     * <b>If this method is invoked before any endpoint is created, the endpoints will not start even though
     * {@link MatsEndpoint#finishSetup()} is invoked on them, but will wait till {@link #start()} is invoked on the
     * factory.</b> This feature should be employed in most setups where the MATS endpoints might use other services or
     * components whose order of creation and initialization are difficult to fully control, e.g. typically in an IoC
     * container like Spring. Set all stuff up, where order is not of importance, and <i>then</i> fire up the endpoints.
     * If this is not done, the endpoints might start consuming messages off of the MQ (there might already be messages
     * waiting when the service boots), and thus invoke services/components that are not yet fully up.
     */
    void holdEndpointsUntilFactoryIsStarted();

    /**
     * Waits until all endpoints are started, i.e. runs {@link MatsEndpoint#waitForStarted()} on all the endpoints
     * started from this factory.
     */
    @Override
    void waitForStarted();

    /**
     * Stops all endpoints and initiators, by invoking {@link MatsEndpoint#stop()} on all the endpoints, and
     * {@link MatsInitiator#close()} on all initiators that has been created by this factory. They can be started again
     * individually, or all at once by invoking {@link #start()}
     * <p>
     * Should be invoked at application shutdown.
     */
    @Override
    void stop();

    /**
     * Provides for a way to configure factory-wide elements and defaults.
     */
    interface FactoryConfig extends MatsConfig {
        /**
         * @return the suggested key on which to store the "wire representation" if the underlying mechanism uses a Map,
         *         e.g. a {@code MapMessage} of JMS. Defaults to <code>"mats:trace"</code>.
         */
        default String getMatsTraceKey() {
            return "mats:trace";
        }

        /**
         * @return the suggested prefix for the messaging system's queues and topics that MATS uses. Defaults to
         *         <code>"mats."</code>.
         */
        default String getMatsDestinationPrefix() {
            return "mats.";
        }

        /**
         * @return the name of the application that employs MATS, set at MatsFactory construction time.
         */
        String getAppName();

        /**
         * @return the version string of the application that employs MATS, set at MatsFactory construction time.
         */
        String getAppVersion();

        /**
         * Returns a node-specific idenitifier, that is, a name which is different between different instances of the
         * same app running of different nodes. This can be used to make node-specific topics, which are nice when you
         * need a message to return to the node that sent it, due to some synchronous process waiting for the message
         * (which entirely defeats the Messaging Oriented Middleware Architecture, but sometimes you need a solution..).
         * This is used in the SynchronousAdapter tool.
         *
         * @return the nodename, which by default should be the hostname which the application is running on.
         */
        String getNodename();
    }
}
