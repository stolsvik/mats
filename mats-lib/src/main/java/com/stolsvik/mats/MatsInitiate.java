package com.stolsvik.mats;

import com.stolsvik.mats.MatsEndpoint.ProcessContext;

/**
 * Get hold of an instance of this interface to initiate a MATS process.
 * <p>
 * To initiate a message "from the outside", i.e. from synchronous application code, get it by invoking
 * {@link MatsFactory#initiate(InitiateLambda)}.
 * <p>
 * To initiate a new message "from the inside", i.e. while already inside a MATS process stage of an endpoint, get it by
 * invoking {@link ProcessContext#initiate(InitiateLambda)}.
 * 
 * @author Endre Stølsvik - 2015-07-11 - http://endre.stolsvik.com
 */
public interface MatsInitiate {

    void traceId(String traceId);

    void from(String endpointId);

    void to(String endpointId);

    void reply(String endpointId);

    void addBinary(String key, byte[] payload);

    void addString(String key, String payload);

    /**
     * The "normal" initiation method: All of from, to and reply must be set. A message is sent to a service, and the
     * reply from that service will come to the specified reply endpointId, typically a terminator.
     */
    void request(Object requestDto, Object replyStateDto);

    /**
     * Variation of the normal initiation method, where the incoming state is sent along. This only makes sense if the
     * same code base "owns" both the initiation code and the endpoint to which this request is sent.
     */
    void request(Object requestDto, Object replyStateDto, Object requestStateDto);

    /**
     * Sends a message to an endpoint, without expecting any reply. Only from and to should be set.
     * 
     * @param requestDto
     *            the object which the endpoint will get as its incomingDto.
     */
    void invoke(Object requestDto);

    /**
     * Variation of the invoke method, where the incoming state is sent along. This only makes sense if the same code
     * base "owns" both the initiation code and the endpoint to which this request is sent.
     * 
     * @param requestDto
     *            the object which the endpoint will get as its incomingDto.
     * @param requestStateDto
     *            the object which the endpoint will get as its stateDto.
     */
    void invoke(Object requestDto, Object requestStateDto);

    /**
     * Sends a message to a {@link MatsFactory#subscriptionTerminator(String, Class, Class, MatsEndpoint.ProcessLambda)
     * SubscriptionTerminator}, employing the publish/subscribe pattern instead of message queues (topic in JMS terms).
     * It is only possible to publish to SubscriptionTerminators as employing publish/subscribe for multi-stage services
     * makes no sense.
     * 
     * @param requestDto
     *            the object which the SubscriptionTerminator will get as its incomingDto.
     */
    void publish(Object requestDto);

    /**
     * Variation of the publish method, where the incoming state is sent along. This only makes sense if the same code
     * base "owns" both the initiation code and the endpoint to which this request is sent.
     * 
     * @param requestDto
     *            the object which the SubscriptionTerminator will get as its incomingDto.
     * @param requestStateDto
     *            the object which the SubscriptionTermiantor will get as its stateDto.
     */
    void publish(Object requestDto, Object requestStateDto);

    @FunctionalInterface
    interface InitiateLambda {
        void initiate(MatsInitiate msg);
    }

}
