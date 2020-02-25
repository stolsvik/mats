-- Note: There is only one row per 'session_id'. The 'connection_id' is a guard against races that can occur when one
-- WebSocket closes and the client immediately reconnects. There might now be two MatsSocketSession instances floating
-- around for the same MatsSocketSessionId, one soon about to understand that his WebSocket Session is closed. To avoid
-- that the "old" session upon realizing this, deregisters the /new/ instance's registration, he must provide his
-- 'connection_id' when deregistering, i.e. it is a guard against the deregister-UPDATE wrt. nodename: The new wants to
-- do a register-"UPSERT" (DELETE-then-INSERT) setting its current nodename, while the old wants to do an
-- deregister-UPDATE setting the nodename to null. The deregister-UPDATE thus has /two/ args in its WHERE clause, so
-- that if the deregister-UPDATE hits after the register-"UPSERT", the deregister-UPDATE's WHERE-clause will hit 0 rows.
CREATE TABLE mats_socket_session
(
    session_id           VARCHAR(255) NOT NULL,
    user_id              VARCHAR(255) NOT NULL, -- An id for the owning user of this session, supplied by the AuthenticationPlugin
    connection_id        VARCHAR(255),          -- NULL if no node has this session anymore. An id for the physical connection, to avoid accidental session deletion upon races. Read above.
    nodename             VARCHAR(255),          -- NULL if no node has this session anymore. The row is deleted when session is closed. Which node ("server") has the live connection.
    created_timestamp    BIGINT       NOT NULL, -- millis since epoch. When the session was originally created.
    liveliness_timestamp BIGINT       NOT NULL, -- millis since epoch. Should be updated upon node-attach, and periodically.

    CONSTRAINT PK_mats_socket_session PRIMARY KEY (session_id)
);

-- :: Going for some good ol' premature optimization:
-- Create 7 outbox tables, hoping that this will reduce contention on the table approximately exactly 7-fold.
-- (7 was chosen based on one finger in the air, and another in the ear, and listening for answers from the ancient ones.)
-- NOTE: MatsSocketSessionId (i.e. 'session_id' in these tables) is the hash-key, using ".hashCode() % 7".

CREATE TABLE mats_socket_outbox_00
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smseq             BIGINT       NOT NULL, -- Server Message Sequence, 'envelope.smseq' - currently random long.
    cmseq             BIGINT,                -- Client Message Sequence, 'envelope.cmseq', if reply to a client message.
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set.
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_00 PRIMARY KEY (session_id, smseq)
);

CREATE TABLE mats_socket_outbox_01
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smseq             BIGINT       NOT NULL, -- Server Message Sequence, 'envelope.smseq' - currently random long.
    cmseq             BIGINT,                -- Client Message Sequence, 'envelope.cmseq', if reply to a client message.
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set.
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_01 PRIMARY KEY (session_id, smseq)
);


CREATE TABLE mats_socket_outbox_02
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smseq             BIGINT       NOT NULL, -- Server Message Sequence, 'envelope.smseq' - currently random long.
    cmseq             BIGINT,                -- Client Message Sequence, 'envelope.cmseq', if reply to a client message.
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set.
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_02 PRIMARY KEY (session_id, smseq)
);

CREATE TABLE mats_socket_outbox_03
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smseq             BIGINT       NOT NULL, -- Server Message Sequence, 'envelope.smseq' - currently random long.
    cmseq             BIGINT,                -- Client Message Sequence, 'envelope.cmseq', if reply to a client message.
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set.
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_03 PRIMARY KEY (session_id, smseq)
);

CREATE TABLE mats_socket_outbox_04
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smseq             BIGINT       NOT NULL, -- Server Message Sequence, 'envelope.smseq' - currently random long.
    cmseq             BIGINT,                -- Client Message Sequence, 'envelope.cmseq', if reply to a client message.
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set.
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_04 PRIMARY KEY (session_id, smseq)
);

CREATE TABLE mats_socket_outbox_05
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smseq             BIGINT       NOT NULL, -- Server Message Sequence, 'envelope.smseq' - currently random long.
    cmseq             BIGINT,                -- Client Message Sequence, 'envelope.cmseq', if reply to a client message.
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set.
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_05 PRIMARY KEY (session_id, smseq)
);

CREATE TABLE mats_socket_outbox_06
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smseq             BIGINT       NOT NULL, -- Server Message Sequence, 'envelope.smseq' - currently random long.
    cmseq             BIGINT,                -- Client Message Sequence, 'envelope.cmseq', if reply to a client message.
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set.
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_06 PRIMARY KEY (session_id, smseq)
);