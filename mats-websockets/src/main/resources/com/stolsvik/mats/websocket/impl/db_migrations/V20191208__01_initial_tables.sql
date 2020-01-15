
-- Note: There is only one row per 'session_id'. The 'connection_id' is a guard against races that can occur when one
-- WebSocket closes and the client immediately reconnects to the same host. There might now be two MatsSocketSession
-- instances floating around in the JVM, one soon about to understand that his WebSocket Session is closed. To avoid
-- that the "old" session upon realizing this, deregisters the /new/ instance's registration, he must provide his
-- 'connection_id' when deregistering, i.e. it is a guard against the DELETE, which thus has /two/ args in its WHERE
-- clause.
CREATE TABLE mats_socket_session (
    session_id VARCHAR(255) NOT NULL,
    connection_id VARCHAR(255), -- An id for the physical connection, to avoid accidental session deletion upon races. Read above.
    user_id VARCHAR(255), -- An id for the owning user of this session, supplied by the AuthenticationPlugin
    nodename VARCHAR(255),  -- NULL if no node has this session anymore. Row is deleted if session closed.
    liveliness_timestamp BIGINT NOT NULL,  -- millis since epoch. Should be updated upon node-attach, and periodically.

    CONSTRAINT PK_mats_socket_session PRIMARY KEY (session_id)
);

CREATE TABLE mats_socket_outbox (
    session_id VARCHAR(255) NOT NULL, -- sessionId which this message belongs to
    message_id BIGINT NOT NULL,  -- random long.
    mseq INT NOT NULL,  -- envelope.[c|s]mseq, [Client|Server] Message Sequence, or -1 if type==MULTI
    trace_id ${texttype} NOT NULL, -- what it says on the tin
    stored_timestamp BIGINT NOT NULL,  -- millis since epoch.
    delivery_count INT NOT NULL, -- Starts at zero.
    type VARCHAR(255) NOT NULL, -- envelope.t, i.e. "type" - or MULTI for a JSON array of messages
    message_text ${texttype}, --
    message_binary ${binarytype},

    CONSTRAINT PK_mats_socket_outbox PRIMARY KEY (session_id, message_id)
);