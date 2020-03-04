package com.stolsvik.mats.websocket;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * MatsSockets forwards requests from WebSocket-connected clients to a Mats Endpoint, and must get the reply back to the
 * client. The WebSocket is "static" in that it is a fixed connection to one specific node (as long as the connection is
 * up), which means that we need to get the message back to the same node that fired off the request. We could have used
 * the same logic as with MatsFuturizer (i.e. the "last jump" uses a node-specific Topic), but that was deemed too
 * feeble: We want reliable messaging all the way to the client. We want to be able to handle that the client looses the
 * connection (e.g. sitting on a train which drives through a tunnel), and thus will reconnect. He might not come back
 * to the same server as last time. We also want to be able to reboot any server (typically deploy of new code) at any
 * time, but this obviously kills all WebSockets that are attached to it. To be able to ensure reliable messaging for
 * MatsSockets, we need to employ a "store and forward" logic: When the reply comes in, which as with standard Mats
 * logic can happen on any node in the cluster of nodes handling this Endpoint Stage, the reply message is temporarily
 * stored in some reliable storage. We then look up which node that currently holds the MatsSession, and notify it about
 * new messages for that session. This node gets the notice, finds the now local MatsSession, and forwards the message.
 * Note that it is possible that the node getting the reply, and the node which holds the WebSocket/MatsSession, is the
 * same node, in which case it is a local forward.
 * <p />
 * Each node has his own instance of this class, connected to the same backing datastore.
 * <p />
 * It is assumed that the consumption of messages for a session is done single threaded, on one node only. That is, only
 * one thread on one node will actually {@link #getMessagesFromOutbox(String, int, boolean)} get messages}, and, more
 * importantly, {@link #outboxMessagesComplete(String, Collection) register dem as completed}. Wrt. multiple nodes, this
 * argument still holds, since only one node can hold a MatsSocket Session. I believe it is possible to construct a bad
 * async situation here (connect to one node, authenticate, get SessionId, immediately disconnect and perform reconnect,
 * and do this until the current {@link ClusterStoreAndForward} has the wrong idea of which node holds the Session) but
 * this should at most result in the client screwing up for himself (not getting messages), and a Session is not
 * registered until the client has authenticated. Such a situation will also resolve if the client again performs a
 * non-malicious reconnect. It is the server that constructs and holds SessionIds: A client cannot itself force the
 * server side to create a Session or SessionId - it can only reconnect to an existing SessionId that it was given
 * earlier.
 *
 * @author Endre Stølsvik 2019-12-07 23:29 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface ClusterStoreAndForward {
    /**
     * Start the {@link ClusterStoreAndForward}, perform DB preparations and migrations.
     */
    void boot();

    /**
     * Registers a Session home to this node - only one node can ever be home, so any old is deleted. When
     * "re-registering" a session, it is asserted that the provided 'userId' is the same UserId as originally registered
     * - an {@link WrongUserException} is thrown if this does not match (Note that this is an extension of
     * {@link DataAccessException}, just out of implementation convenience).
     *
     * @param matsSocketSessionId
     *            the SessionId for this connection.
     * @param userId
     *            the UserId, as provided by {@link AuthenticationPlugin}, that own this MatsSocketSessionId
     * @param connectionId
     *            an id that is unique for this specific WebSocket Session (i.e. TCP Connection), so that if it closes,
     *            a new registration will not be deregistered by the old MatsSocketSession realizing that it is closed
     *            and then invoking {@link #deregisterSessionFromThisNode(String, String)}
     * @throws WrongUserException
     *             if the userId provided does not match the original userId that created the session.
     * @throws DataAccessException
     *             if problems with underlying data store.
     */
    void registerSessionAtThisNode(String matsSocketSessionId, String userId, String connectionId)
            throws WrongUserException, DataAccessException;

    /**
     * @param matsSocketSessionId
     *            the MatsSocketSessionId for which to find current session home.
     * @return the current node holding MatsSocket Session, or empty if none.
     */
    Optional<CurrentNode> getCurrentRegisteredNodeForSession(String matsSocketSessionId) throws DataAccessException;

    /**
     * Shall be invoked on some kind of schedule (e.g. every 5 minute) by node to inform {@link ClusterStoreAndForward}
     * about Sessions' liveliness. Sessions that aren't live, will be scavenged after some time, e.g. 24 hours.
     *
     * @param matsSocketSessionIds
     */
    void notifySessionLiveliness(Collection<String> matsSocketSessionIds) throws DataAccessException;

    /**
     * @param matsSocketSessionId
     *            the MatsSocketSessionId for which to check whether there is a session
     * @return whether a CSAF Session exists - NOTE: Not whether it is currently registered!
     */
    boolean isSessionExists(String matsSocketSessionId) throws DataAccessException;

    /**
     * Deregisters a Session home when a WebSocket is closed. This node's nodename and this WebSocket Session's
     * ConnectionId is taken into account, so that if it has changed async, it will not deregister a new session home
     * (which can potentially be on the same node the 'connectionId' parameter).
     *
     * @param matsSocketSessionId
     *            the MatsSocketSessionId for which to deregister the specific WebSocket Session's ConnectionId.
     * @param connectionId
     *            an id that is unique for this specific WebSocket Session (i.e. TCP Connection), so that if it closes,
     *            a new registration will not be deregistered by the old MatsSocketSession realizing that it is closed
     *            and then invoking {@link #deregisterSessionFromThisNode(String, String)}
     */
    void deregisterSessionFromThisNode(String matsSocketSessionId, String connectionId)
            throws DataAccessException;

    /**
     * Invoked when the client explicitly tells us that he closed this session, CLOSE_SESSION. This deletes the session
     * instance, and any messages in queue for it. No new incoming REPLYs, SENDs or RECEIVEs for this SessionId will be
     * sent anywhere. (Notice that the implementation might still store any new outboxed messages, not checking that the
     * session actually exists. But this SessionId will never be reused (exception without extreme "luck"), and the
     * implementation will periodically purge such non-session-attached outbox messages).
     *
     * @param matsSocketSessionId
     *            the MatsSocketSessionId that should be closed.
     */
    void closeSession(String matsSocketSessionId) throws DataAccessException;

    /**
     * Stores the incoming message Id, to avoid double delivery. If the messageId already exists, a
     * {@link ClientMessageIdAlreadyExistsException} will be raised.
     *
     * @param matsSocketSessionId
     *            the MatsSocketSessionId for which to store the incoming message id.
     * @param clientMessageId
     *            the client's message Id for the incoming message.
     * @throws ClientMessageIdAlreadyExistsException
     *             if this messageId already existed for this SessionId.
     */
    void storeMessageIdInInbox(String matsSocketSessionId, String clientMessageId)
            throws ClientMessageIdAlreadyExistsException, DataAccessException;

    /**
     * Deletes the incoming message Ids, as we've established that the client will never try to send this particular
     * message again.
     *
     * @param matsSocketSessionId
     *            the MatsSocketSessionId for which to delete the incoming message id.
     * @param clientMessageIds
     *            the client's message Ids for the incoming messages to delete.
     */
    void deleteMessageIdsFromInbox(String matsSocketSessionId, Collection<String> clientMessageIds)
            throws DataAccessException;

    /**
     * Stores the message for the Session, returning the nodename for the node holding the session, if any. If the
     * session is closed/timed out, the message won't be stored (i.e. dumped on the floor) and the return value is
     * empty. The ServerMessageId is set by the CSAF, and available when
     * {@link #getMessagesFromOutbox(String, int, boolean) getting messages}.
     *
     * @param matsSocketSessionId
     *            the matsSocketSessionId that the message is meant for.
     * @param traceId
     *            the server-side traceId for this message.
     * @param clientMessageId
     *            the envelope.cmid
     * @param type
     *            the type of the reply, currently "REPLY".
     * @param message
     *            the JSON-serialized MatsSocket <b>Envelope</b>.
     * @return the current node holding MatsSocket Session, or empty if none.
     */
    Optional<CurrentNode> storeMessageInOutbox(String matsSocketSessionId, String traceId,
            String clientMessageId, String type, String message) throws DataAccessException;

    /**
     * Fetch a set of messages, up to 'maxNumberOfMessages'.
     *
     * @param matsSocketSessionId
     *            the matsSocketSessionId that the message is meant for.
     * @param maxNumberOfMessages
     *            the maximum number of messages to fetch.
     * @param takeAlreadyAttempted
     *            if <code>true</code>, instead of excluding already attempted message, now only pick these.
     * @return a list of json encoded messages destined for the WebSocket.
     */
    List<StoredMessage> getMessagesFromOutbox(String matsSocketSessionId, int maxNumberOfMessages,
            boolean takeAlreadyAttempted) throws DataAccessException;

    /**
     * States that the messages are delivered. Will typically delete the message.
     *
     * @param matsSocketSessionId
     *            the matsSocketSessionId that the serverMessageIds refers to.
     * @param serverMessageIds
     *            which messages are complete.
     */
    void outboxMessagesComplete(String matsSocketSessionId, Collection<String> serverMessageIds)
            throws DataAccessException;

    /**
     * Notches the 'delivery_count' one up for the specified messages.
     *
     * @param matsSocketSessionId
     *            the matsSocketSessionId that the serverMessageIds refers to.
     * @param serverMessageIds
     *            which messages failed delivery.
     */
    void outboxMessagesAttemptedDelivery(String matsSocketSessionId, Collection<String> serverMessageIds)
            throws DataAccessException;

    /**
     * States that the messages overran the accepted number of delivery attempts.
     *
     * @param matsSocketSessionId
     *            the matsSocketSessionId that the serverMessageIds refers to.
     * @param serverMessageIds
     *            which messages should be DLQed.
     */
    void outboxMessagesDeadLetterQueue(String matsSocketSessionId, Collection<String> serverMessageIds)
            throws DataAccessException;

    /**
     * Thrown from {@link #registerSessionAtThisNode(String, String, String)} if the userId does not match the original
     * userId that created this session.
     */
    class WrongUserException extends Exception {
        public WrongUserException(String message) {
            super(message);
        }
    }

    /**
     * Thrown if the operation resulted in a Unique Constraint situation. Relevant for
     * {@link #storeMessageIdInInbox(String, String)}.
     */
    class ClientMessageIdAlreadyExistsException extends Exception {
        public ClientMessageIdAlreadyExistsException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * If having problems accessing the underlying common data store.
     */
    class DataAccessException extends Exception {
        public DataAccessException(String message) {
            super(message);
        }

        public DataAccessException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    interface CurrentNode {
        String getNodename();

        String getConnectionId();
    }

    class SimpleCurrentNode implements CurrentNode {
        private final String nodename;
        private final String connectionId;

        public SimpleCurrentNode(String nodename, String connectionId) {
            this.nodename = nodename;
            this.connectionId = connectionId;
        }

        @Override
        public String getNodename() {
            return nodename;
        }

        @Override
        public String getConnectionId() {
            return connectionId;
        }
    }

    interface CsafSession {
        String getSessionId();

        String getUserId();

        String getNodename();

        String getConnectionId();

        long getCreatedTimestamp();

        long getLivelinessTimestamp();

    }

    class SimpleCsafSession implements CsafSession {
        private final String sessionId;
        private final String userId;
        private final String nodename;
        private final String connectionId;
        private final long createdTimestamp;
        private final long livelinessTimestamp;

        public SimpleCsafSession(String sessionId, String userId, String nodename, String connectionId,
                long createdTimestamp,
                long livelinessTimestamp) {
            this.sessionId = sessionId;
            this.userId = userId;
            this.nodename = nodename;
            this.connectionId = connectionId;
            this.createdTimestamp = createdTimestamp;
            this.livelinessTimestamp = livelinessTimestamp;
        }

        @Override
        public String getSessionId() {
            return sessionId;
        }

        @Override
        public String getUserId() {
            return userId;
        }

        @Override
        public String getNodename() {
            return nodename;
        }

        @Override
        public String getConnectionId() {
            return connectionId;
        }

        @Override
        public long getCreatedTimestamp() {
            return createdTimestamp;
        }

        @Override
        public long getLivelinessTimestamp() {
            return livelinessTimestamp;
        }
    }

    interface StoredMessage {
        String getMatsSocketSessionId();

        String getServerMessageId();

        Optional<String> getClientMessageId();

        long getStoredTimestamp();

        Optional<Long> getAttemptTimestamp();

        int getDeliveryCount();

        String getTraceId();

        String getType();

        String getEnvelopeJson();
    }

    class SimpleStoredMessage implements StoredMessage {
        private final String _matsSocketSessionId;
        private final String _serverMessageSequence;
        private final String _clientMessageId;
        private final long _storedTimestamp;
        private final Long _attemptTimestamp;
        private final int _deliveryCount;

        private final String _traceId;
        private final String _type;
        private final String _envelopeJson;

        public SimpleStoredMessage(String matsSocketSessionId, String serverMessageSequence,
                String clientMessageId,
                long storedTimestamp, Long attemptTimestamp, int deliveryCount, String traceId, String type,
                String envelopeJson) {
            _matsSocketSessionId = matsSocketSessionId;
            _serverMessageSequence = serverMessageSequence;
            _clientMessageId = clientMessageId;
            _storedTimestamp = storedTimestamp;
            _attemptTimestamp = attemptTimestamp;
            _deliveryCount = deliveryCount;
            _traceId = traceId;
            _type = type;
            _envelopeJson = envelopeJson;
        }

        @Override
        public String getMatsSocketSessionId() {
            return _matsSocketSessionId;
        }

        @Override
        public String getServerMessageId() {
            return _serverMessageSequence;
        }

        @Override
        public Optional<String> getClientMessageId() {
            return Optional.ofNullable(_clientMessageId);
        }

        @Override
        public long getStoredTimestamp() {
            return _storedTimestamp;
        }

        @Override
        public Optional<Long> getAttemptTimestamp() {
            return Optional.ofNullable(_attemptTimestamp);
        }

        @Override
        public int getDeliveryCount() {
            return _deliveryCount;
        }

        @Override
        public String getTraceId() {
            return _traceId;
        }

        @Override
        public String getType() {
            return _type;
        }

        @Override
        public String getEnvelopeJson() {
            return _envelopeJson;
        }
    }
}
