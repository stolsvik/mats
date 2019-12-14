
-- Note: Only one row per 'mats_session_id'. The 'connection_id' is a guard against races that can occur when one
-- WebSocket closes and the client immediately reconnects to the same host. There might now be two MatsSocketSession
-- instances floating around in the JVM, one soon about to understand that his WebSocket Session is closed. To avoid
-- that he upon realizing this, deregisters the /new/ instance's registration, he must provide his 'connection_id'
-- when deregistering.
CREATE TABLE mats_socket_session (
    mats_session_id VARCHAR(255) NOT NULL,
    connection_id VARCHAR(255), -- An id for the physical connection, to avoid races. Read above.
    nodename VARCHAR(255),  -- NULL if no node has this session anymore. Row is deleted if terminated.
    liveliness_timestamp BIGINT NOT NULL,  -- millis since epoch.

    CONSTRAINT PK_mats_socket_session PRIMARY KEY (mats_session_id)
);

CREATE TABLE mats_socket_message (
    mats_session_id VARCHAR(255) NOT NULL,
    message_id BIGINT NOT NULL,
    trace_id ${texttype} NOT NULL,
    stored_timestamp BIGINT NOT NULL,  -- millis since epoch.
    delivery_count INT NOT NULL, -- Starts at zero.
    type VARCHAR(255) NOT NULL,
    message_text ${texttype},
    message_binary ${binarytype},

    CONSTRAINT PK_mats_socket_message PRIMARY KEY (mats_session_id, message_id)
);