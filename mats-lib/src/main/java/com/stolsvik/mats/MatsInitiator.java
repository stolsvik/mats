package com.stolsvik.mats;

import java.io.Closeable;
import java.util.UUID;

import org.slf4j.MDC;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;

/**
 * Provides a way to get a {@link MatsInitiate} instance "from the outside" of MATS, i.e. from a synchronous context. On
 * this instance, you invoke {@link #initiate(InitiateLambda)}, where the lambda will provide you with the necessary
 * {@link MatsInitiate} instance.
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public interface MatsInitiator extends Closeable {

    /**
     * Initiates a new message (request or invocation) out to an endpoint.
     *
     * @param lambda
     *            provides the {@link MatsInitiate} instance on which to create the message to be sent.
     */
    void initiate(InitiateLambda lambda);

    @FunctionalInterface
    interface InitiateLambda {
        void initiate(MatsInitiate msg);
    }

    /**
     * Closes any underlying backend resource.
     */
    @Override
    void close();

    /**
     * You must have access to an instance of this interface to initiate a MATS process.
     * <p>
     * To initiate a message "from the outside", i.e. from synchronous application code, get it by invoking
     * {@link MatsFactory#getInitiator(String)}, and then {@link MatsInitiator#initiate(InitiateLambda)} on that.
     * <p>
     * To initiate a new message "from the inside", i.e. while already inside a {@link MatsStage processing stage} of an
     * endpoint, get it by invoking {@link ProcessContext#initiate(InitiateLambda)}.
     *
     * @author Endre Stølsvik - 2015-07-11 - http://endre.stolsvik.com
     */
    public interface MatsInitiate {

        /**
         * Sets (or appends with a "|" in case of {@link ProcessContext#initiate(InitiateLambda) initiation within a
         * stage}) the "Trace Id", which is solely used for logging and debugging purposes.
         * <p>
         * Since it is so important when doing asynchronous architectures, it is mandatory.
         * <p>
         * The traceId follows a MATS processing from the initiation until it is finished, usually in a Terminator.
         * <p>
         * It is set on the {@link MDC} of the SLF4J logging system, using the key "matsTraceId".
         *
         * @param traceId
         *            some world-unique Id, preferably set all the way back when some actual person performed some event
         *            (e.g. in a "new order" situation, the Id would best be set when the user clicked the "place order"
         *            button - or maybe even derived from the event when he first initiated the shopping cart - or maybe
         *            even when he started the session. The point is that when using e.g. Kibana or Splunk to track
         *            events that led some some outcome, a robust, versatile and information-rich track/trace Id makes
         *            wonders).
         * @return the {@link MatsInitiate} for chaining.
         */
        MatsInitiate traceId(String traceId);

        /**
         * Overrides the Initiator Id that was set either with {@link MatsFactory#getInitiator(String)}, or implicitly
         * by the Endpoint (stage) Id from which {@link ProcessContext#initiate(InitiateLambda)} was invoked.
         *
         * @param initiatorId
         *            a fictive "endpointId" representing the "initiating endpoint".
         * @return the {@link MatsInitiate} for chaining.
         */
        MatsInitiate from(String initiatorId);

        /**
         * Sets which MATS Endpoint this message should go.
         *
         * @param endpointId
         *            to which MATS Endpoint this message should go.
         * @return the {@link MatsInitiate} for chaining.
         */
        MatsInitiate to(String endpointId);

        /**
         * Specified which MATS Endpoint the reply of the invoked Endpoint should go to.
         *
         * @param endpointId
         *            which MATS Endpoint the reply of the invoked Endpoint should go to.
         * @return the {@link MatsInitiate} for chaining.
         */
        MatsInitiate replyTo(String endpointId);

        /**
         * Adds a binary payload to the endpoint, e.g. a PDF document.
         *
         * @param key
         *            the key on which this is set. A typical logic is to just use an {@link UUID} as key, and then
         *            reference the payload key in the Request DTO.
         * @param payload
         *            the byte array.
         * @return the {@link MatsInitiate} for chaining.
         */
        MatsInitiate addBinary(String key, byte[] payload);

        /**
         * Adds a String payload to the endpoint, e.g. a XML document.
         * <p>
         * The rationale for having this is to not have to encode a largish string document inside the JSON structure
         * that carries the Request DTO.
         *
         * @param key
         *            the key on which this is set. A typical logic is to just use an {@link UUID} as key, and then
         *            reference the payload key in the Request DTO.
         * @param payload
         *            the string.
         * @return the {@link MatsInitiate} for chaining.
         */
        MatsInitiate addString(String key, String payload);

        /**
         * <i>The standard request initiation method</i>: All of from, to and replyTo must be set. A message is sent to
         * a service, and the reply from that service will come to the specified reply endpointId, typically a
         * terminator.
         *
         * @param requestDto
         *            the object which the endpoint will get as its incoming DTO (Data Transfer Object).
         * @param replySto
         *            the object that should be provided as STO to the service which get the reply.
         */
        void request(Object requestDto, Object replySto);

        /**
         * <b>Variation of the request initiation method</b>, where the incoming state is sent along.
         * <p>
         * <b>This only makes sense if the same code base "owns" both the initiation code and the endpoint to which this
         * message is sent.</b> It is mostly here for completeness, since it is <i>possible</i> to send state along with
         * the message, but if employed between different services, it violates the premise that MATS is built on: State
         * is private to the stages of a multi-stage endpoint, and the Request and Reply DTOs are the public interface.
         *
         * @param requestDto
         *            the object which the endpoint will get as its incoming DTO (Data Transfer Object).
         * @param replySto
         *            the object that should be provided as STO to the service which get the reply.
         * @param requestSto
         *            the object which the endpoint will get as its STO (State Transfer Object).
         */
        void request(Object requestDto, Object replySto, Object requestSto);

        /**
         * Sends a message to an endpoint, without expecting any reply ("fire-and-forget"). The 'reply' parameter must
         * not be set.
         * <p>
         * Note that the difference between request and invoke is only that replyTo is not set for invoke, otherwise the
         * mechanism is exactly the same.
         *
         * @param requestDto
         *            the object which the endpoint will get as its incoming DTO (Data Transfer Object).
         */
        void send(Object requestDto);

        /**
         * <b>Variation of the {@link #send(Object)} method</b>, where the incoming state is sent along.
         * <p>
         * <b>This only makes sense if the same code base "owns" both the initiation code and the endpoint to which this
         * message is sent.</b> It is mostly here for completeness, since it is <i>possible</i> to send state along with
         * the message, but if employed between different services, it violates the premise that MATS is built on: State
         * is private to the stages of a multi-stage endpoint, and the Request and Reply DTOs are the public interface.
         *
         * @param requestDto
         *            the object which the endpoint will get as its incoming DTO (Data Transfer Object).
         * @param requestSto
         *            the object which the endpoint will get as its STO (State Transfer Object).
         */
        void send(Object requestDto, Object requestSto);

        /**
         * Sends a message to a
         * {@link MatsFactory#subscriptionTerminator(String, Class, Class, MatsEndpoint.ProcessLambda)
         * SubscriptionTerminator}, employing the publish/subscribe pattern instead of message queues (topic in JMS
         * terms). <b>This means that all of the live servers that are listening to this endpointId will receive the
         * message, and if there are no live servers, then no one will receive it.</b>
         * <p>
         * The concurrency of a SubscriptionTerminator is always 1, as it only makes sense for there being only one
         * receiver per server - otherwise it would just mean that all of the active listeners on one server would get
         * the message, per semantics of the pub/sub.
         * <p>
         * It is only possible to publish to SubscriptionTerminators as employing publish/subscribe for multi-stage
         * services makes no sense.
         *
         * @param requestDto
         *            the object which the endpoint will get as its incoming DTO (Data Transfer Object).
         */
        void publish(Object requestDto);

        /**
         * <b>Variation of the {@link #publish(Object)} method</b>, where the incoming state is sent along.
         * <p>
         * <b>This only makes sense if the same code base "owns" both the initiation code and the endpoint to which this
         * message is sent.</b> The possibility to send state along with the request makes most sense with the publish
         * method: A SubscriptionTerminator is often paired with a Terminator, where the Terminator receives the
         * message, typically a reply from a requested service, along with the state that was sent in the initiation.
         * The terminator does any "needs to be guaranteed to be performed" state changes to e.g. database, and then
         * passes the request - <b>along with the same state it received</b> - on to a SubscriptionTerminator. The
         * SubscriptionTerminator performs any updates of any connected GUI clients, or for any other local states, e.g.
         * invalidation of caches, <i>on all live servers listening to that endpoint</i>, the point being that if no
         * servers are live at that moment, no one will process that message - but at the same time, there is obviously
         * no GUI clients connected, nor are there are local state in form of caches that needs to be invalidated.
         *
         * @param requestDto
         *            the object which the endpoint will get as its incoming DTO (Data Transfer Object).
         * @param requestSto
         *            the object which the endpoint will get as its STO (State Transfer Object).
         */
        void publish(Object requestDto, Object requestSto);
    }
}
