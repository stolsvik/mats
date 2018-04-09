package com.stolsvik.mats.serial;

import com.stolsvik.mats.MatsFactory;

import java.util.List;

/**
 * Interface-representation of the underlying "wire protocol" which Mats is running on. Which implementation of the wire
 * format is being employed is configured when creating the <code>MatsFactory</code>, and which formats are available
 * are dependent on the underlying transport (typically JMS).
 * <p>
 * From the outset, there is one format (JSON serialization of the <code>MatsTrace_DefaultJson</code> class using
 * Jackson), and one transport (JMS). Notice that the serialization of the actual DTOs and STOs can be specified
 * independently, e.g. use GSON.
 *
 * @param <Z> The type which STOs and DTOs are serialized into.
 *
 * @author Endre Stølsvik - 2018-03-17 23:37, factored out from original from 2015 - http://endre.stolsvik.com
 */
public interface MatsTrace<Z> {
    String getTraceId();

    /**
     * @return whether trace should be kept, default is <code>false</code>.
     */
    boolean isKeepTrace();

    /**
     * @return whether the message should be JMS-style "non-persistent", default is <code>false</code> (i.e. persistent,
     * reliable).
     */
    boolean isNonPersistent();

    /**
     * @return whether the message should be prioritized in that a human is actively waiting for the reply, default
     * is <code>false</code>.
     */
    boolean isInteractive();

    void setTraceProperty(String propertyName, Z propertyValue);

    Z getTraceProperty(String propertyName);


    MatsTrace<Z> addRequestCall(String from, String to, Z data, List<String> replyStack, Z replyState, Z initialState);

    MatsTrace<Z> addSendCall(String from, String to, Z data, List<String> replyStack, Z initialState);

    MatsTrace<Z> addNextCall(String from, String to, Z data, List<String> replyStack, Z state);

    MatsTrace<Z> addReplyCall(String from, String to, Z data, List<String> replyStack);


    Call<Z> getCurrentCall();

    Z getCurrentState();

    enum CallType {
        REQUEST,

        SEND,

        NEXT,

        REPLY
    }

    /**
     * Represents an immutable entry in the {@link MatsTrace}.
     */
    interface Call<Z> {
        CallType getType();

        String getFrom();

        String getTo();

        Z getData();

        /**
         * @return a COPY of the stack. The LAST element is the most recent, the 0th element is the first in the stack,
         * i.e. the stageId where the terminator typically will reside (that is, if present due to an initial Request
         * with a reply-to).
         */
        List<String> getStack();
    }
}
