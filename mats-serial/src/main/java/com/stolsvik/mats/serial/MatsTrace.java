package com.stolsvik.mats.serial;

import java.util.List;

/**
 * Together with the {@link MatsSerializer}, this interface describes one way to implement a wire-protocol for how Mats
 * communicates. It is up to the implementation of the <code>MatsFactory</code> to implement a protocol of how the Mats
 * API is transferred over the wire. This is one such implementation that can be used, which is employed by the default
 * JMS implementation of Mats.
 * <p>
 * From the outset, there is one format (JSON serialization of the <code>MatsTrace_DefaultJson</code> class using
 * Jackson), and one transport (JMS). Notice that the serialization of the actual DTOs and STOs can be specified
 * independently, e.g. use GSON, or protobuf, or whatever can handle serialization to and from the DTOs and STOs being
 * used.
 *
 * @param <Z> The type which STOs and DTOs are serialized into. When employing JSON for the "outer" serialization of
 *            MatsTrace, it does not make that much sense to use a binary (Z=byte[]) "inner" representation of the DTOs
 *            and STOs, because JSON is terrible at serializing byte arrays.
 *
 * @author Endre Stølsvik - 2018-03-17 23:37, factored out from original from 2015 - http://endre.stolsvik.com
 */
public interface MatsTrace<Z> {
    /**
     * @return the TraceId that this {@link MatsTrace} was initiated with - this is set once, at initiation time, and
     *         follows the processing till it terminates. (All log lines will have the traceId set on the MDC.)
     */
    String getTraceId();

    /**
     * @return to which extent the Call history (with State) should be kept. The default is
     *         {@link KeepMatsTrace#COMPACT}.
     */
    KeepMatsTrace getKeepTrace();

    /**
     * Specifies how the MatsTrace will handle historic values that are present just for debugging. Notice the annoyance
     * that this is effectively specified twice, once in the MATS API, and once here. That is better, IMHO, than this
     * package depending on the MATS API only for that enum.
     */
    enum KeepMatsTrace {
        /**
         * Keeps all Data and State history for the entire trace. (Still the {@link Call#getFrom() from} and
         * {@link Call#getStack() stack} will be nulled, as it provide zero value that cannot be deduced from the
         * previous calls).
         */
        FULL,

        /**
         * <b>Default</b>: Nulls out Data for other than current call while still keeping the meta-info for the call
         * history, and condenses State to a pure stack.
         */
        COMPACT,

        /**
         * Only keep the {@link MatsTrace#getCurrentCall() current call}, and condenses State to a pure stack.
         */
        MINIMAL
    }

    /**
     * @return whether the message should be JMS-style "non-persistent", default is <code>false</code> (i.e. persistent,
     *         reliable).
     */
    boolean isNonPersistent();

    /**
     * @return whether the message should be prioritized in that a human is actively waiting for the reply, default is
     *         <code>false</code>.
     */
    boolean isInteractive();

    /**
     * Can only be set once..
     */
    MatsTrace<Z> setDebugInfo(String initializingAppName, String initializingAppVersion, String initializingHost,
            String initiatorId, long initializedTimestamp, String debugInfo);

    String getInitializingAppName();

    String getInitializingAppVersion();

    String getInitializingHost();

    String getInitiatorId();

    long getInitializedTimestamp();

    String getDebugInfo();

    /**
     * Sets a trace property, refer to <code>ProcessContext.setTraceProperty(String, Object)</code>. Notice that on the
     * MatsTrace-side, the value must be a String.
     *
     * @param propertyName
     *            the name of the property.
     * @param propertyValue
     *            the value of the property.
     */
    void setTraceProperty(String propertyName, Z propertyValue);

    /**
     * Retrieves a property value set by {@link #setTraceProperty(String, Z)}, refer to
     * <code>ProcessContext.getTraceProperty(String, Class)</code>. Notice that on the MatsTrace-side, the value is a
     * String.
     *
     * @param propertyName
     *            the name of the property to retrieve.
     * @return the value of the property.
     */
    Z getTraceProperty(String propertyName);

    /**
     * Adds a {@link Call.CallType#REQUEST REQUEST} Call, which is an invocation of a service where one expects a Reply
     * from this service to go to a specified endpoint, typically the next stage in a multi-stage endpoint: Envision a
     * normal invocation of some method that returns a value.
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param to
     *            which endpoint that should get the request.
     * @param replyTo
     *            which endpoint that should get the reply from the requested endpoint.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the requesting
     *            service expects to get.
     * @param replyState
     *            the state data for the stageId that gets the reply to this request, that is, the state for the stageId
     *            that is at the first element of the replyStack. Most often a JSON representing the State Transfer
     *            Object for the multi-stage endpoint.
     * @param initialState
     *            an optional feature, whereby the state can be set for the initial stage of the requested endpoint.
     *            Same stuff as replyState.
     */
    MatsTrace<Z> addRequestCall(String from, String to, String replyTo, Z data, Z replyState, Z initialState);

    /**
     * Adds a {@link Call.CallType#SEND SEND} Call, meaning a "request" which do not expect a Reply: Envision an
     * invocation of a void-method. Or an invocation of some method that returns the value, but where you invoke it as a
     * void-method (i.e. not storing the result, e.g. the method <code>map.remove("test")</code> returns the removed
     * value, but is often invoked without storing this.).
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param to
     *            which endpoint that should get the message.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the receiving
     *            service expects to get.
     * @param initialState
     *            an optional feature, whereby the state can be set for the initial stage of the requested endpoint.
     */
    MatsTrace<Z> addSendCall(String from, String to, Z data, Z initialState);

    /**
     * Adds a {@link Call.CallType#NEXT NEXT} Call, which is a "downwards call" to the next stage in a multistage
     * service, as opposed to the normal request out to a service expecting a reply. The functionality is functionally
     * identical to {@link #addSendCall(String, String, Z, Z) addSendCall(...)}, but has its own
     * {@link Call.CallType CallType} enum {@link Call.CallType#NEXT value}.
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param to
     *            which endpoint that should get the message - the next stage in a multi-stage service.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the next stage
     *            expects to get.
     * @param state
     *            the state data for the next stage.
     */
    MatsTrace<Z> addNextCall(String from, String to, Z data, Z state);

    /**
     * Adds a {@link Call.CallType#REPLY REPLY} Call, which happens when a requested service is finished with its
     * processing and have some Reply to return. This method pops the stack (takes the last element) from the (previos)
     * current call, sets this as the "to" parameter, and uses the rest of the list as the stack for the next Call.
     *
     * @param from
     *            which stageId this request is for. This is solely meant for monitoring and debugging - the protocol
     *            does not need the from specifier, as this is not where any replies go to.
     * @param data
     *            the request data, most often a JSON representing the Request Data Transfer Object that the requesting
     *            service expects to get.
     */
    MatsTrace<Z> addReplyCall(String from, Z data);

    /**
     * @return the {@link Call Call} which should be processed by the stage receiving this {@link MatsTrace} (which
     *         should be the stageId specified in getCurrentCall().{@link Call#getTo() getTo()}).
     */
    Call<Z> getCurrentCall();

    /**
     * @return the flow of calls, from the first REQUEST (or SEND), to the {@link #getCurrentCall() current call} -
     *         unless {@link #getKeepTrace() KeepTrace} is MINIMAL, in which case only the current call is present in
     *         the list.
     */
    List<Call<Z>> getCallFlow();

    /**
     * @return the state for the {@link #getCurrentCall()}.
     */
    Z getCurrentState();

    /**
     * @return the stack of the states for the current stack: getCurrentCall().getStack().
     */
    List<StackState<String>> getStateStack();

    /**
     * @return the entire list of states as they have changed throughout the call flow. If {@link KeepMatsTrace} is
     *         COMPACT or MINIMAL, then it will be a pure stack (as returned with {@link #getStateStack()}, with the
     *         last element being the most recent stack frame.
     */
    List<StackState<Z>> getStateFlow();

    /**
     * Represents an immutable entry in the {@link MatsTrace}.
     */
    interface Call<Z> {
        /**
         * Can only be set once..
         */
        Call<Z> setDebugInfo(String callingAppName, String callingAppVersion, String callingHost,
                long calledTimestamp, String debugInfo);

        String getCallingAppName();

        String getCallingAppVersion();

        String getCallingHost();

        long getCalledTimestamp();

        String getDebugInfo();

        CallType getCallType();

        enum CallType {
            REQUEST,

            SEND,

            NEXT,

            REPLY
        }

        /**
         * @return the stageId that sent this call - will most probably be the string "-nulled-" for any other Call than
         *         the {@link MatsTrace#getCurrentCall()}, to conserve space in the MatsTrace. The rationale for this,
         *         is that if those Calls are available, they are there for debug purposes only, and then you can use
         *         the order of the Calls to see who is the caller: The previous Call's {@link #getTo() "to"} is the
         *         {@link #getFrom() "from"} of this Call.
         */
        String getFrom();

        String getTo();

        Z getData();

        /**
         * @return the size of the stack for this Call - which is the number of elements <i>below</i> this call. I.e.
         *         for a {@link CallType#REPLY REPLY} to a Terminator, the stack is of size 0 (there are no more
         *         elements to REPLY to), while for the first {@link CallType#REQUEST REQUEST} from an initiator, the
         *         stack is of size 1 (the endpointId for the Terminator is the one element below this Call).
         */
        int getStackSize();

        /**
         * @return a COPY of the stack (if you need the size, use {@link #getStackSize()}) - NOTICE: This will most
         *         probably be a List with {@link #getStackSize()} elements containing "-nulled-" for any other Call
         *         than the {@link MatsTrace#getCurrentCall()}, to conserve space in the MatsTrace. The LAST (i.e.
         *         position 'size()-1') element is the most recent, meaning that the next REPLY will go here, while the
         *         FIRST (i.e. position 0) element is the earliest in the stack, i.e. the stageId where the Terminator
         *         endpointId typically will reside (unless the initial call was a {@link CallType#SEND SEND}, which
         *         means that you don't want a reply).
         */
        List<String> getStack();
    }

    /**
     * The State instances (of type Z), along with the depth of the stack the state relates to.
     */
    interface StackState<Z> {
        int getHeight();

        Z getState();
    }
}
