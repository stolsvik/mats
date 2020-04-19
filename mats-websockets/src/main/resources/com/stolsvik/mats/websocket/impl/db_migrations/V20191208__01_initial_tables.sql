-- == Session Registrations ==
-- Note: There is only one row per 'session_id'. The 'connection_id' is a guard against races that can occur when one
-- WebSocket closes and the client immediately reconnects. There might now be two MatsSocketSession instances floating
-- around for the same MatsSocketSessionId, one soon about to understand that his WebSocket Session is closed. To avoid
-- that the "old" session deregisters the /new/ instance's registration when realizing this, he must provide his
-- 'connection_id' when deregistering, i.e. it is a guard against the deregister-UPDATE wrt. nodename: The new wants to
-- do a register-"UPSERT" (DELETE-then-INSERT) setting its current nodename, while the old wants to do an
-- deregister-UPDATE setting the nodename to null. The deregister-UPDATE therefore has /two/ args in its WHERE clause,
-- so that if the deregister-UPDATE hits after the register-"UPSERT", the deregister-UPDATE's WHERE-clause will hit 0
-- rows.
CREATE TABLE mats_socket_session
(
    session_id           VARCHAR(255) NOT NULL,
    connection_id        VARCHAR(255),          -- NULL if no node has this session anymore. An id for the physical connection, to avoid accidental session deletion upon races. Read above.
    nodename             VARCHAR(255),          -- NULL if no node has this session anymore. The row is deleted when session is closed. Which node ("server") has the live connection.
    user_id              VARCHAR(255) NOT NULL, -- An id for the owning user of this session, supplied by the AuthenticationPlugin
    client_lib           VARCHAR(255) NOT NULL, -- 'clv': ClientLibAndVersions
    app_name             VARCHAR(255) NOT NULL, -- The AppName of the accessing Client app
    app_version          VARCHAR(255) NOT NULL, -- The AppVersion of the accessing Client app
    created_timestamp    BIGINT       NOT NULL, -- millis since epoch. When the session was originally created.
    liveliness_timestamp BIGINT       NOT NULL, -- millis since epoch. Should be updated upon node-attach, and periodically.

    CONSTRAINT PK_mats_socket_session PRIMARY KEY (session_id)
);

-- == The INBOX ==
-- To recognize a client-to-server redelivery of an already processed messages, i.e. "double delivery catcher".
-- All SENDs and REQUESTs from Client-to-Server get an entry here (information bearing messages).
-- NOTICE: Going for some good ol' premature optimization:
--   Create 7 outbox tables, hoping that this will reduce contention on the table approximately exactly 7-fold.
--   (7 was chosen based on one finger in the air, and another in the ear, and listening for answers from the ancient ones.)
--   Hash-key is MatsSocketSessionId (i.e. 'session_id' in these tables), using ".hashCode() % 7".
CREATE TABLE mats_socket_inbox_00
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    cmid              VARCHAR(255) NOT NULL, -- Client Message Id, 'envelope.cmid'
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    full_envelope     ${texttype},           -- Envelope including 'msg' field set.
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_inbox_00 PRIMARY KEY (session_id, cmid)
);

CREATE TABLE mats_socket_inbox_01
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    cmid              VARCHAR(255) NOT NULL, -- Client Message Id, 'envelope.cmid'
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    full_envelope     ${texttype},           -- Envelope including 'msg' field set.
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_inbox_01 PRIMARY KEY (session_id, cmid)
);

CREATE TABLE mats_socket_inbox_02
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    cmid              VARCHAR(255) NOT NULL, -- Client Message Id, 'envelope.cmid'
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    full_envelope     ${texttype},           -- Envelope including 'msg' field set.
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_inbox_02 PRIMARY KEY (session_id, cmid)
);

CREATE TABLE mats_socket_inbox_03
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    cmid              VARCHAR(255) NOT NULL, -- Client Message Id, 'envelope.cmid'
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    full_envelope     ${texttype},           -- Envelope including 'msg' field set.
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_inbox_03 PRIMARY KEY (session_id, cmid)
);

CREATE TABLE mats_socket_inbox_04
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    cmid              VARCHAR(255) NOT NULL, -- Client Message Id, 'envelope.cmid'
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    full_envelope     ${texttype},           -- Envelope including 'msg' field set.
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_inbox_04 PRIMARY KEY (session_id, cmid)
);

CREATE TABLE mats_socket_inbox_05
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    cmid              VARCHAR(255) NOT NULL, -- Client Message Id, 'envelope.cmid'
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    full_envelope     ${texttype},           -- Envelope including 'msg' field set.
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_inbox_05 PRIMARY KEY (session_id, cmid)
);

CREATE TABLE mats_socket_inbox_06
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    cmid              VARCHAR(255) NOT NULL, -- Client Message Id, 'envelope.cmid'
    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    full_envelope     ${texttype},           -- Envelope including 'msg' field set.
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_inbox_06 PRIMARY KEY (session_id, cmid)
);

-- == The OUTBOX ==
-- To store outbound messages to enable retransmission if failure.
-- Also using good'ol premature optimizations.
CREATE TABLE mats_socket_outbox_00
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid              VARCHAR(255) NOT NULL, -- Server Message Id, 'envelope.smid' - a random, quite small string
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'

    cmid              VARCHAR(255),          -- Client Message Id, 'envelope.cmid', if reply to a client request.
    request_timestamp BIGINT,                -- If Reply to a Client Request: when request received, millis since epoch.

    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch. Nulled to perform retransmission.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set. Magic number '-666' if DLQ.

    envelope          ${texttype}  NOT NULL, -- The envelope of the message, without the 'msg' field set.
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_00 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_outbox_01
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid              VARCHAR(255) NOT NULL, -- Server Message Id, 'envelope.smid' - a random, quite small string
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'

    cmid              VARCHAR(255),          -- Client Message Id, 'envelope.cmid', if reply to a client request.
    request_timestamp BIGINT,                -- If Reply to a Client Request: when request received, millis since epoch.

    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch. Nulled to perform retransmission.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set. Magic number '-666' if DLQ.

    envelope          ${texttype}  NOT NULL, -- The envelope of the message, without the 'msg' field set.
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_01 PRIMARY KEY (session_id, smid)
);


CREATE TABLE mats_socket_outbox_02
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid              VARCHAR(255) NOT NULL, -- Server Message Id, 'envelope.smid' - a random, quite small string
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'

    cmid              VARCHAR(255),          -- Client Message Id, 'envelope.cmid', if reply to a client request.
    request_timestamp BIGINT,                -- If Reply to a Client Request: when request received, millis since epoch.

    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch. Nulled to perform retransmission.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set. Magic number '-666' if DLQ.

    envelope          ${texttype}  NOT NULL, -- The envelope of the message, without the 'msg' field set.
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_02 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_outbox_03
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid              VARCHAR(255) NOT NULL, -- Server Message Id, 'envelope.smid' - a random, quite small string
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'

    cmid              VARCHAR(255),          -- Client Message Id, 'envelope.cmid', if reply to a client request.
    request_timestamp BIGINT,                -- If Reply to a Client Request: when request received, millis since epoch.

    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch. Nulled to perform retransmission.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set. Magic number '-666' if DLQ.

    envelope          ${texttype}  NOT NULL, -- The envelope of the message, without the 'msg' field set.
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_03 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_outbox_04
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid              VARCHAR(255) NOT NULL, -- Server Message Id, 'envelope.smid' - a random, quite small string
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'

    cmid              VARCHAR(255),          -- Client Message Id, 'envelope.cmid', if reply to a client request.
    request_timestamp BIGINT,                -- If Reply to a Client Request: when request received, millis since epoch.

    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch. Nulled to perform retransmission.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set. Magic number '-666' if DLQ.

    envelope          ${texttype}  NOT NULL, -- The envelope of the message, without the 'msg' field set.
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_04 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_outbox_05
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid              VARCHAR(255) NOT NULL, -- Server Message Id, 'envelope.smid' - a random, quite small string
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'

    cmid              VARCHAR(255),          -- Client Message Id, 'envelope.cmid', if reply to a client request.
    request_timestamp BIGINT,                -- If Reply to a Client Request: when request received, millis since epoch.

    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch. Nulled to perform retransmission.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set. Magic number '-666' if DLQ.

    envelope          ${texttype}  NOT NULL, -- The envelope of the message, without the 'msg' field set.
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_05 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_outbox_06
(
    session_id        VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid              VARCHAR(255) NOT NULL, -- Server Message Id, 'envelope.smid' - a random, quite small string
    trace_id          ${texttype}  NOT NULL, -- what it says on the tin
    type              VARCHAR(255) NOT NULL, -- Type of message, 'envelope.t'

    cmid              VARCHAR(255),          -- Client Message Id, 'envelope.cmid', if reply to a client request.
    request_timestamp BIGINT,                -- If Reply to a Client Request: when request received, millis since epoch.

    stored_timestamp  BIGINT       NOT NULL, -- When the message was stored here. millis since epoch.
    attempt_timestamp BIGINT,                -- When an attempt at delivery was performed. millis since epoch. Nulled to perform retransmission.
    delivery_count    INT          NOT NULL, -- Starts at zero - increased each time 'attempt_timestamp' is set. Magic number '-666' if DLQ.

    envelope          ${texttype}  NOT NULL, -- The envelope of the message, without the 'msg' field set.
    message_text      ${texttype},
    message_binary    ${binarytype},

    CONSTRAINT PK_mats_socket_outbox_06 PRIMARY KEY (session_id, smid)
);

-- == The "REQUEST BOX" ==
-- Storage for outgoing REQUESTs Server-to-Client, to store the CorrelationString and CorrelationBinary, and timestamp.
-- Also using good'ol premature optimizations (even though this might be overkill for Server-to-Client requests due to less use..)
CREATE TABLE mats_socket_request_out_00
(
    session_id          VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid                VARCHAR(255) NOT NULL, -- Server Message Id of the outgoing REQUEST, 'envelope.smid'
    request_timestamp   BIGINT       NOT NULL, -- millis since epoch. When this REQUEST was originally done.
    reply_terminator_id VARCHAR(255),          -- If this is a request, then this is where the reply should go.
    correlation_text    ${texttype},           -- 'correlationSpecifier' in MatsSocketServer.request(...)
    correlation_binary  ${binarytype},         -- 'correlationSpecifier' in MatsSocketServer.request(...)

    CONSTRAINT PK_mats_socket_request_out_00 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_request_out_01
(
    session_id          VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid                VARCHAR(255) NOT NULL, -- Server Message Id of the outgoing REQUEST, 'envelope.smid'
    request_timestamp   BIGINT       NOT NULL, -- millis since epoch. When this REQUEST was originally done.
    reply_terminator_id VARCHAR(255),          -- If this is a request, then this is where the reply should go.
    correlation_text    ${texttype},           -- 'correlationSpecifier' in MatsSocketServer.request(...)
    correlation_binary  ${binarytype},         -- 'correlationSpecifier' in MatsSocketServer.request(...)

    CONSTRAINT PK_mats_socket_request_out_01 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_request_out_02
(
    session_id          VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid                VARCHAR(255) NOT NULL, -- Server Message Id of the outgoing REQUEST, 'envelope.smid'
    request_timestamp   BIGINT       NOT NULL, -- millis since epoch. When this REQUEST was originally done.
    reply_terminator_id VARCHAR(255),          -- If this is a request, then this is where the reply should go.
    correlation_text    ${texttype},           -- 'correlationSpecifier' in MatsSocketServer.request(...)
    correlation_binary  ${binarytype},         -- 'correlationSpecifier' in MatsSocketServer.request(...)

    CONSTRAINT PK_mats_socket_request_out_02 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_request_out_03
(
    session_id          VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid                VARCHAR(255) NOT NULL, -- Server Message Id of the outgoing REQUEST, 'envelope.smid'
    request_timestamp   BIGINT       NOT NULL, -- millis since epoch. When this REQUEST was originally done.
    reply_terminator_id VARCHAR(255),          -- If this is a request, then this is where the reply should go.
    correlation_text    ${texttype},           -- 'correlationSpecifier' in MatsSocketServer.request(...)
    correlation_binary  ${binarytype},         -- 'correlationSpecifier' in MatsSocketServer.request(...)

    CONSTRAINT PK_mats_socket_request_out_03 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_request_out_04
(
    session_id          VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid                VARCHAR(255) NOT NULL, -- Server Message Id of the outgoing REQUEST, 'envelope.smid'
    request_timestamp   BIGINT       NOT NULL, -- millis since epoch. When this REQUEST was originally done.
    reply_terminator_id VARCHAR(255),          -- If this is a request, then this is where the reply should go.
    correlation_text    ${texttype},           -- 'correlationSpecifier' in MatsSocketServer.request(...)
    correlation_binary  ${binarytype},         -- 'correlationSpecifier' in MatsSocketServer.request(...)

    CONSTRAINT PK_mats_socket_request_out_04 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_request_out_05
(
    session_id          VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid                VARCHAR(255) NOT NULL, -- Server Message Id of the outgoing REQUEST, 'envelope.smid'
    request_timestamp   BIGINT       NOT NULL, -- millis since epoch. When this REQUEST was originally done.
    reply_terminator_id VARCHAR(255),          -- If this is a request, then this is where the reply should go.
    correlation_text    ${texttype},           -- 'correlationSpecifier' in MatsSocketServer.request(...)
    correlation_binary  ${binarytype},         -- 'correlationSpecifier' in MatsSocketServer.request(...)

    CONSTRAINT PK_mats_socket_request_out_05 PRIMARY KEY (session_id, smid)
);

CREATE TABLE mats_socket_request_out_06
(
    session_id          VARCHAR(255) NOT NULL, -- sessionId which this message belongs to.
    smid                VARCHAR(255) NOT NULL, -- Server Message Id of the outgoing REQUEST, 'envelope.smid'
    request_timestamp   BIGINT       NOT NULL, -- millis since epoch. When this REQUEST was originally done.
    reply_terminator_id VARCHAR(255),          -- If this is a request, then this is where the reply should go.
    correlation_text    ${texttype},           -- 'correlationSpecifier' in MatsSocketServer.request(...)
    correlation_binary  ${binarytype},         -- 'correlationSpecifier' in MatsSocketServer.request(...)

    CONSTRAINT PK_mats_socket_request_out_06 PRIMARY KEY (session_id, smid)
);



