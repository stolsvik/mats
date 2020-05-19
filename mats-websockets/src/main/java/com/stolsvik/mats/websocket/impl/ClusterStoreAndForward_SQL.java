package com.stolsvik.mats.websocket.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import javax.sql.DataSource;

import com.stolsvik.mats.websocket.ClusterStoreAndForward;
import com.stolsvik.mats.websocket.MatsSocketServer.MatsSocketSessionDto;
import com.stolsvik.mats.websocket.MatsSocketServer.MessageType;

/**
 * An implementation of CSAF relying on a shared SQL database to store the necessary information in a cluster setting.
 * <p/>
 * <b>NOTE: This CSAF implementation expects that the database tables are in place.</b> A tool is provided for this,
 * using Flyway: {@link ClusterStoreAndForward_SQL_DbMigrations}.
 * <p/>
 * <b>NOTE: If in a Spring JDBC environment, where the MatsFactory is created using the
 * <code>JmsMatsTransactionManager_JmsAndSpringDstm</code> Mats transaction manager, you must wrap the
 * {@link DataSource} supplied to this class in a Spring <code>TransactionAwareDataSourceProxy</code>.</b> This since
 * several of the methods on this interface will be invoked within a Mats process lambda, and thus participating in the
 * transactional demarcation established there is vital to achieve the guaranteed delivery and exactly-once delivery
 * semantics.
 *
 * @author Endre Stølsvik 2019-12-08 11:00 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ClusterStoreAndForward_SQL implements ClusterStoreAndForward {
    private static final int DLQ_DELIVERY_COUNT_MARKER = -666;

    private static final String INBOX_TABLE_PREFIX = "mats_socket_inbox_";

    private static final String OUTBOX_TABLE_PREFIX = "mats_socket_outbox_";

    private static final String REQUEST_OUT_TABLE_PREFIX = "mats_socket_request_out_";

    private static final int NUMBER_OF_BOX_TABLES = 7;

    private final DataSource _dataSource;
    private final String _nodename;
    private final Clock _clock;

    public static ClusterStoreAndForward_SQL create(DataSource dataSource, String nodename) {
        ClusterStoreAndForward_SQL csaf = new ClusterStoreAndForward_SQL(dataSource, nodename);
        return csaf;
    }

    protected ClusterStoreAndForward_SQL(DataSource dataSource, String nodename, Clock clock) {
        _dataSource = dataSource;
        _nodename = nodename;
        _clock = clock;
    }

    private ClusterStoreAndForward_SQL(DataSource dataSource, String nodename) {
        this(dataSource, nodename, Clock.systemDefaultZone());
    }

    @Override
    public void boot() {
        // TODO: Implement rudimentary assertions here: Register a session, add some messages, fetch them, etc..
    }

    // ---------- Session management ----------

    @Override
    public long registerSessionAtThisNode(String matsSocketSessionId, String userId, String connectionId,
            String clientLibAndVersions, String appName, String appVersion)
            throws WrongUserException, DataAccessException {
        try (Connection con = _dataSource.getConnection()) {
            boolean autoCommitPre = con.getAutoCommit();
            try { // turn back autocommit, just to be sure we've not changed state of connection.

                // ?: If transactional-mode was not on, turn it on now (i.e. autoCommit->false)
                // NOTE: Otherwise, we assume an outside transaction demarcation is in effect.
                if (autoCommitPre) {
                    // Start transaction
                    con.setAutoCommit(false);
                }

                // :: Check if the Session already exists.
                PreparedStatement select = con.prepareStatement("SELECT user_id, created_timestamp"
                        + " FROM mats_socket_session WHERE session_id = ?");
                select.setString(1, matsSocketSessionId);
                ResultSet rs = select.executeQuery();
                long createdTimestamp;
                long now;
                // ?: Did we get a row on the SessionId?
                if (rs.next()) {
                    // -> Yes, we did - so get the original userId, and the original createdTimestamp
                    String originalUserId = rs.getString(1);
                    createdTimestamp = rs.getLong(2);
                    // ?: Has the userId changed from the original userId?
                    if (!userId.equals(originalUserId)) {
                        // -> Yes, changed: This is bad stuff - drop out right now.
                        throw new WrongUserException("The original userId of MatsSocketSessionId ["
                                + matsSocketSessionId + "] was [" + originalUserId
                                + "], while the new one that attempts to reconnect to session is [" + userId + "].");
                    }
                    now = _clock.millis();
                }
                else {
                    createdTimestamp = now = _clock.millis();
                }
                select.close();

                // :: Generic "UPSERT" implementation: DELETE-then-INSERT (no need for SELECT/UPDATE-or-INSERT here)
                // Unconditionally delete session (the INSERT puts in the new values).
                PreparedStatement delete = con.prepareStatement("DELETE FROM mats_socket_session"
                        + " WHERE session_id = ?");
                delete.setString(1, matsSocketSessionId);

                // Insert the new current row
                PreparedStatement insert = con.prepareStatement("INSERT INTO mats_socket_session"
                        + "(session_id, connection_id, nodename, user_id, client_lib, app_name, app_version,"
                        + " created_timestamp, liveliness_timestamp)"
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
                insert.setString(1, matsSocketSessionId);
                insert.setString(2, connectionId);
                insert.setString(3, _nodename);
                insert.setString(4, userId);
                insert.setString(5, clientLibAndVersions);
                insert.setString(6, appName);
                insert.setString(7, appVersion);
                insert.setLong(8, createdTimestamp);
                insert.setLong(9, now);

                // Execute them both
                delete.execute();
                insert.execute();
                delete.close();
                insert.close();

                // ?: If we turned off autocommit, we should commit now.
                if (autoCommitPre) {
                    // Commit transaction.
                    con.commit();
                }

                return createdTimestamp;
            }
            finally {
                // ?: If we changed the autoCommit to false to get transaction (since it was true), we turn it back now.
                if (autoCommitPre) {
                    con.setAutoCommit(true);
                }
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Got '" + e.getClass().getSimpleName() + "' accessing DataSource.", e);
        }
    }

    @Override
    public void deregisterSessionFromThisNode(String matsSocketSessionId, String connectionId)
            throws DataAccessException {
        withConnectionVoid(con -> {
            // Note that we include a "WHERE nodename=<thisnode> AND connection_id=<specified connectionId>"
            // here, so as to not mess up if he has already re-registered with new socket, or on a new node.
            PreparedStatement update = con.prepareStatement("UPDATE mats_socket_session"
                    + "   SET nodename = NULL"
                    + " WHERE session_id = ?"
                    + "   AND connection_id = ?"
                    + "   AND nodename = ?");
            update.setString(1, matsSocketSessionId);
            update.setString(2, connectionId);
            update.setString(3, _nodename);
            update.execute();
            update.close();
        });
    }

    @Override
    public Optional<CurrentNode> getCurrentRegisteredNodeForSession(String matsSocketSessionId)
            throws DataAccessException {
        return withConnectionReturn(con -> _getCurrentNode(matsSocketSessionId, con, true));
    }

    private Optional<CurrentNode> _getCurrentNode(String matsSocketSessionId, Connection con, boolean onlyIfHasNode)
            throws SQLException {
        PreparedStatement select = con.prepareStatement("SELECT nodename, connection_id FROM mats_socket_session"
                + " WHERE session_id = ?");
        select.setString(1, matsSocketSessionId);
        try {
            ResultSet resultSet = select.executeQuery();
            boolean next = resultSet.next();
            if (!next) {
                return Optional.empty();
            }
            String nodename = resultSet.getString(1);
            if ((nodename == null) && onlyIfHasNode) {
                return Optional.empty();
            }
            String connectionId = resultSet.getString(2);
            return Optional.of(new SimpleCurrentNode(nodename, connectionId));
        }
        finally {
            select.close();
        }
    }

    @Override
    public boolean isSessionExists(String matsSocketSessionId) throws DataAccessException {
        return withConnectionReturn(con -> _getCurrentNode(matsSocketSessionId, con, false).isPresent());
    }

    private PreparedStatement _prepareSessionSelectSql(Connection con, boolean justCount, boolean onlyActive,
            String userId, String appName, String appVersionAtOrAbove) throws SQLException {

        // Create SQL
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT ");
        if (justCount) {
            buf.append("COUNT(1) ");
        }
        else {
            buf.append("session_id, nodename, user_id, client_lib, app_name, app_version,"
                    + " created_timestamp, liveliness_timestamp ");
        }
        buf.append(" FROM mats_socket_session\n");
        buf.append("  WHERE 1=1\n");
        if (onlyActive) {
            buf.append("   AND nodename IS NOT NULL\n");
        }
        if (userId != null) {
            buf.append("   AND user_id = ?\n");
        }
        if (appName != null) {
            buf.append("   AND app_name = ?\n");
        }
        if (appVersionAtOrAbove != null) {
            buf.append("   AND app_version >= ?\n");
        }

        // Create PreparedStatement with resulting SQL
        PreparedStatement select = con.prepareStatement(buf.toString());

        // Set parameters on statement, handling the index crap.
        int paramIdx = 1;
        if (userId != null) {
            select.setString(paramIdx, userId);
            paramIdx++;
        }
        if (appName != null) {
            select.setString(paramIdx, appName);
            paramIdx++;
        }
        if (appVersionAtOrAbove != null) {
            select.setString(paramIdx, appVersionAtOrAbove);
        }

        return select;
    }

    @Override
    public List<MatsSocketSessionDto> getSessions(boolean onlyActive, String userId, String appName,
            String appVersionAtOrAbove) throws DataAccessException {
        return withConnectionReturn(con -> {
            PreparedStatement select = _prepareSessionSelectSql(con, false, onlyActive, userId,
                    appName, appVersionAtOrAbove);
            ResultSet rs = select.executeQuery();
            List<MatsSocketSessionDto> sessions = new ArrayList<>();
            while (rs.next()) {
                MatsSocketSessionDto session = new MatsSocketSessionDto();
                session.id = rs.getString(1);
                session.uid = rs.getString(3);
                session.scts = rs.getLong(7);
                session.slts = rs.getLong(8);
                session.clv = rs.getString(4);
                session.an = rs.getString(5);
                session.av = rs.getString(6);
                session.nn = rs.getString(2);

                sessions.add(session);
            }
            select.close();
            return sessions;
        });
    }

    @Override
    public int getSessionsCount(boolean onlyActive, String userId, String appName, String appVersionAtOrAbove)
            throws DataAccessException {
        return withConnectionReturn(con -> {
            PreparedStatement select = _prepareSessionSelectSql(con, true, onlyActive, userId,
                    appName, appVersionAtOrAbove);
            ResultSet rs = select.executeQuery();
            if (!rs.next()) {
                throw new AssertionError("Missing ResultSet for COUNT(1)!");
            }
            return rs.getInt(1);
        });
    }

    @Override
    public void closeSession(String matsSocketSessionId) throws DataAccessException {
        withConnectionVoid(con -> {
            // Notice that we DO NOT include WHERE nodename is us. User asked us to delete, and that we do.
            PreparedStatement deleteSession = con.prepareStatement("DELETE FROM mats_socket_session"
                    + " WHERE session_id = ?");
            deleteSession.setString(1, matsSocketSessionId);
            deleteSession.execute();
            deleteSession.close();

            PreparedStatement deleteInbox = con.prepareStatement("DELETE FROM " + inboxTableName(matsSocketSessionId)
                    + " WHERE session_id = ?");
            deleteInbox.setString(1, matsSocketSessionId);
            deleteInbox.execute();
            deleteInbox.close();

            PreparedStatement deleteOutbox = con.prepareStatement("DELETE FROM " + outboxTableName(matsSocketSessionId)
                    + " WHERE session_id = ?");
            deleteOutbox.setString(1, matsSocketSessionId);
            deleteOutbox.execute();
            deleteOutbox.close();
        });
    }

    @Override
    public void notifySessionLiveliness(Collection<String> matsSocketSessionIds) throws DataAccessException {
        withConnectionVoid(con -> {
            long now = _clock.millis();
            // TODO / OPTIMIZE: Make "in" optimizations.
            PreparedStatement update = con.prepareStatement("UPDATE mats_socket_session"
                    + "   SET liveliness_timestamp = ?"
                    + " WHERE session_id = ?");
            for (String matsSocketSessionId : matsSocketSessionIds) {
                update.setLong(1, now);
                update.setString(2, matsSocketSessionId);
                update.addBatch();
            }
            update.executeBatch();
            update.close();
        });
    }

    @Override
    public Collection<String> timeoutSessions(long notLiveSinceTimestamp) throws DataAccessException {
        return withConnectionReturn(con -> {

            /*
             * First mark the sessions to be timed out in a quick update, based on the liveliness timestamp. This so
             * that we can select them out for return from this method. Then delete them, based on marker. Using the
             * liveliness timestamp both as criteria, and as marker.
             *
             * 1. We find ourselves a random NEGATIVE long, which will be the marker.
             *
             * 2. UPDATE session, marking for timeout, based on the liveliness timestamp vs. criteria, setting the
             * liveliness timestamp to the marker value.
             *
             * 3. SELECT out which sessions this is, based on the marker value.
             *
             * 4. DELETE the sessions, based on the marker value.
             *
             * 5. Return the sessionIds that we deleted.
             *
             * The rationale for using a random marker is that there might be multiple nodes doing a "timeout round"
             * concurrently, and to avoid notifying about the same session being timed out on multiple nodes, we use use
             * (hopefully) different markers for each node. Half of long's value space as random value for something
             * that probably very seldom actually will overlap is so abundantly sufficient that I cannot be bothered
             * thinking more about it.
             */

            long markerValue = -(Math.abs(ThreadLocalRandom.current().nextLong()));
            if (markerValue == 0) {
                markerValue = -1337;
            }

            PreparedStatement markUpdate = con.prepareStatement("UPDATE mats_socket_session"
                    + "   SET liveliness_timestamp = ?"
                    + " WHERE liveliness_timestamp < ?");
            markUpdate.setLong(1, markerValue);
            markUpdate.setLong(2, notLiveSinceTimestamp);
            markUpdate.execute();
            int numTimedOutSessions = markUpdate.getUpdateCount();
            markUpdate.close();

            List<String> timedOutSessionids = new ArrayList<>(numTimedOutSessions);
            // ?: Was any rows updated, i.e. timed out?
            if (numTimedOutSessions > 0) {
                // -> Yes, there was timed out rows.

                // SELECT out the affected MatsSocketSessionIds
                PreparedStatement select = con.prepareStatement("SELECT session_id FROM mats_socket_session"
                        + " WHERE liveliness_timestamp = ?");
                select.setLong(1, markerValue);
                ResultSet rs = select.executeQuery();
                while (rs.next()) {
                    timedOutSessionids.add(rs.getString(1));
                }
                select.close();

                // DELETE all the affected MatsSocketSessions
                // NOTE: Any lingering messages in inbox, outbox or "request box" will be scavenged later..
                PreparedStatement delete = con.prepareStatement("DELETE FROM mats_socket_session"
                        + " WHERE LIVELINESS_TIMESTAMP = ?");
                delete.setLong(1, markerValue);
                delete.execute();
                delete.close();
            }

            return timedOutSessionids;
        });
    }

    @Override
    public int scavengeSessionRemnants() throws DataAccessException {
        String whereSessionIdNotExists = " AS m WHERE NOT EXISTS"
                + " (SELECT 1 FROM mats_socket_session AS s WHERE s.session_id = m.session_id)";
        return withConnectionReturn((con) -> {
            Statement stmt = con.createStatement();
            int count = 0;
            // :: Perform scavenge from all box-tables
            for (int i = 0; i < NUMBER_OF_BOX_TABLES; i++) {
                stmt.execute("DELETE FROM " + inboxTableName(i) + whereSessionIdNotExists);
                count += stmt.getUpdateCount();
                stmt.execute("DELETE FROM " + outboxTableName(i) + whereSessionIdNotExists);
                count += stmt.getUpdateCount();
                stmt.execute("DELETE FROM " + requestOutTableName(i) + whereSessionIdNotExists);
                count += stmt.getUpdateCount();
            }
            stmt.close();
            return count;
        });
    }

    // ---------- Inbox ----------

    @Override
    public void storeMessageIdInInbox(String matsSocketSessionId, String clientMessageId)
            throws ClientMessageIdAlreadyExistsException, DataAccessException {
        try (Connection con = _dataSource.getConnection()) {
            PreparedStatement insert = con.prepareStatement("INSERT INTO " + inboxTableName(matsSocketSessionId)
                    + " (session_id, cmid, stored_timestamp)"
                    + " VALUES (?, ?, ?)");
            insert.setString(1, matsSocketSessionId);
            insert.setString(2, clientMessageId);
            insert.setLong(3, System.currentTimeMillis());
            insert.execute();
            insert.close();
        }
        catch (SQLIntegrityConstraintViolationException e) {
            throw new ClientMessageIdAlreadyExistsException("Could not insert the ClientMessageId [" + clientMessageId
                    + "] for MatsSocketSessionId [" + matsSocketSessionId + "].", e);
        }
        catch (SQLException e) {
            throw new DataAccessException("Got '" + e.getClass().getSimpleName() + "' accessing DataSource.", e);
        }
    }

    @Override
    public void updateMessageInInbox(String matsSocketSessionId, String clientMessageId, String envelopeWithMessage,
            byte[] messageBinary) throws DataAccessException {
        withConnectionVoid(con -> {
            PreparedStatement updateMsg = con.prepareStatement("UPDATE " + inboxTableName(matsSocketSessionId)
                    + " SET full_envelope = ?, message_binary = ?"
                    + " WHERE session_id = ?"
                    + "   AND cmid = ?");
            updateMsg.setString(1, envelopeWithMessage);
            updateMsg.setBytes(2, messageBinary);
            updateMsg.setString(3, matsSocketSessionId);
            updateMsg.setString(4, clientMessageId);
            updateMsg.execute();
            updateMsg.close();
        });
    }

    @Override
    public StoredInMessage getMessageFromInbox(String matsSocketSessionId,
            String clientMessageId) throws DataAccessException {
        return withConnectionReturn(con -> {
            PreparedStatement select = con.prepareStatement("SELECT"
                    + " stored_timestamp, full_envelope, message_binary"
                    + "  FROM " + inboxTableName(matsSocketSessionId)
                    + " WHERE session_id = ?"
                    + "   AND cmid = ?");
            select.setString(1, matsSocketSessionId);
            select.setString(2, clientMessageId);
            ResultSet rs = select.executeQuery();
            rs.next();

            SimpleStoredInMessage msg = new SimpleStoredInMessage(matsSocketSessionId,
                    clientMessageId, rs.getLong(1), rs.getString(2),
                    rs.getBytes(3));
            select.close();
            return msg;
        });
    }

    @Override
    public void deleteMessageIdsFromInbox(String matsSocketSessionId, Collection<String> clientMessageIds)
            throws DataAccessException {
        withConnectionVoid(con -> {
            // TODO / OPTIMIZE: Make "in" optimizations.
            PreparedStatement deleteMsg = con.prepareStatement("DELETE FROM " + inboxTableName(matsSocketSessionId)
                    + " WHERE session_id = ?"
                    + "   AND cmid = ?");
            for (String messageId : clientMessageIds) {
                deleteMsg.setString(1, matsSocketSessionId);
                deleteMsg.setString(2, messageId);
                deleteMsg.addBatch();
            }
            deleteMsg.executeBatch();
            deleteMsg.close();
        });
    }

    // ---------- Outbox ----------

    @Override
    public Optional<CurrentNode> storeMessageInOutbox(String matsSocketSessionId, String serverMessageId,
            String clientMessageId, String traceId, MessageType type, Long requestTimestamp, String envelope,
            String messageJson, byte[] messageBinary) throws DataAccessException {
        return withConnectionReturn(con -> {
            PreparedStatement insert = con.prepareStatement("INSERT INTO " + outboxTableName(matsSocketSessionId)
                    + "(session_id, smid, trace_id, type,"
                    + " cmid, request_timestamp,"
                    + " stored_timestamp, delivery_count,"
                    + " envelope, message_text, message_binary)"
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            insert.setString(1, matsSocketSessionId);
            insert.setString(2, serverMessageId);
            insert.setString(3, traceId);
            insert.setString(4, type.name());

            insert.setString(5, clientMessageId); // May be null
            insert.setLong(6, requestTimestamp); // May be null

            insert.setLong(7, _clock.millis());
            insert.setInt(8, 0);

            insert.setString(9, envelope);
            insert.setString(10, messageJson); // May be null
            insert.setBytes(11, messageBinary); // May be null
            insert.execute();

            return _getCurrentNode(matsSocketSessionId, con, true);
        });
    }

    @Override
    public List<StoredOutMessage> getMessagesFromOutbox(String matsSocketSessionId, int maxNumberOfMessages)
            throws DataAccessException {
        return withConnectionReturn(con -> {
            // The old MS JDBC Driver 'jtds' don't handle parameter insertion for 'TOP' statement.
            PreparedStatement insert = con.prepareStatement("SELECT TOP " + maxNumberOfMessages
                    + " smid, trace_id, type,"
                    + " cmid, request_timestamp,"
                    + " stored_timestamp, delivery_count,"
                    + " envelope, message_text, message_binary"
                    + "  FROM " + outboxTableName(matsSocketSessionId)
                    + " WHERE session_id = ?"
                    + "   AND attempt_timestamp IS NULL"
                    + "   AND delivery_count <> " + DLQ_DELIVERY_COUNT_MARKER);
            insert.setString(1, matsSocketSessionId);
            ResultSet rs = insert.executeQuery();
            List<StoredOutMessage> list = new ArrayList<>();
            while (rs.next()) {
                MessageType type = MessageType.valueOf(rs.getString(3));
                SimpleStoredOutMessage sm = new SimpleStoredOutMessage(matsSocketSessionId, rs.getString(1),
                        rs.getString(2), type,
                        rs.getString(4), rs.getLong(5),
                        rs.getLong(6), null, rs.getInt(7),
                        rs.getString(8), rs.getString(9), rs.getBytes(10));
                list.add(sm);
            }
            return list;
        });
    }

    @Override
    public void outboxMessagesAttemptedDelivery(String matsSocketSessionId, Collection<String> serverMessageIds)
            throws DataAccessException {
        long now = _clock.millis();
        withConnectionVoid(con -> {
            PreparedStatement update = con.prepareStatement("UPDATE " + outboxTableName(matsSocketSessionId)
                    + "   SET attempt_timestamp = ?,"
                    + "       delivery_count = delivery_count + 1"
                    + " WHERE session_id = ?"
                    + "   AND smid = ?");
            for (String messageId : serverMessageIds) {
                update.setLong(1, now);
                update.setString(2, matsSocketSessionId);
                update.setString(3, messageId);
                update.addBatch();
            }
            update.executeBatch();
        });
    }

    @Override
    public void outboxMessagesUnmarkAttemptedDelivery(String matsSocketSessionId) throws DataAccessException {
        withConnectionVoid(con -> {
            PreparedStatement update = con.prepareStatement("UPDATE " + outboxTableName(matsSocketSessionId)
                    + "   SET attempt_timestamp = NULL"
                    + " WHERE session_id = ?");
            update.setString(1, matsSocketSessionId);
            update.addBatch();
            update.executeBatch();
        });
    }

    @Override
    public void outboxMessagesComplete(String matsSocketSessionId, Collection<String> serverMessageIds)
            throws DataAccessException {
        withConnectionVoid(con -> {
            // TODO / OPTIMIZE: Make "in" optimizations.
            PreparedStatement deleteMsg = con.prepareStatement("DELETE FROM " + outboxTableName(matsSocketSessionId)
                    + " WHERE session_id = ?"
                    + "   AND smid = ?");
            for (String messageId : serverMessageIds) {
                deleteMsg.setString(1, matsSocketSessionId);
                deleteMsg.setString(2, messageId);
                deleteMsg.addBatch();
            }
            deleteMsg.executeBatch();
            deleteMsg.close();
        });
    }

    @Override
    public void outboxMessagesDeadLetterQueue(String matsSocketSessionId, Collection<String> serverMessageIds)
            throws DataAccessException {
        withConnectionVoid(con -> {
            // TODO / OPTIMIZE: Make "in" optimizations.
            PreparedStatement update = con.prepareStatement("UPDATE " + outboxTableName(matsSocketSessionId)
                    + "   SET attempt_timestamp = " + DLQ_DELIVERY_COUNT_MARKER
                    + " WHERE session_id = ?"
                    + "   AND smid = ?");
            for (String messageId : serverMessageIds) {
                update.setString(1, matsSocketSessionId);
                update.setString(2, messageId);
                update.addBatch();
            }
            update.executeBatch();
        });
    }

    // ---------- "Request box" ----------

    @Override
    public void storeRequestCorrelation(String matsSocketSessionId, String serverMessageId, long requestTimestamp,
            String replyTerminatorId, String correlationString, byte[] correlationBinary) throws DataAccessException {
        withConnectionVoid(con -> {
            PreparedStatement insert = con.prepareStatement("INSERT INTO " + requestOutTableName(matsSocketSessionId)
                    + " (session_id, smid, request_timestamp, reply_terminator_id, correlation_text, correlation_binary)"
                    + " VALUES (?, ?, ?, ?, ?, ?)");
            insert.setString(1, matsSocketSessionId);
            insert.setString(2, serverMessageId);
            insert.setLong(3, requestTimestamp);
            insert.setString(4, replyTerminatorId);
            insert.setString(5, correlationString);
            insert.setBytes(6, correlationBinary);
            insert.execute();
            insert.close();
        });
    }

    @Override
    public Optional<RequestCorrelation> getRequestCorrelation(String matsSocketSessionId,
            String serverMessageId) throws DataAccessException {
        return withConnectionReturn(con -> {
            PreparedStatement insert = con.prepareStatement("SELECT "
                    + " request_timestamp, reply_terminator_id, correlation_text, correlation_binary"
                    + "  FROM " + requestOutTableName(matsSocketSessionId)
                    + " WHERE session_id = ?"
                    + " AND smid = ?");
            insert.setString(1, matsSocketSessionId);
            insert.setString(2, serverMessageId);
            ResultSet rs = insert.executeQuery();
            // ?: Did we get a result? (Shall only be one, due to unique constraint in SQL DDL).
            if (rs.next()) {
                // -> Yes, we have the row!
                // Get the data
                RequestCorrelation requestCorrelation = new SimpleRequestCorrelation(matsSocketSessionId,
                        serverMessageId, rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBytes(4));
                // Return the data.
                return Optional.of(requestCorrelation);
            }
            // E-> No, no result - so empty.
            return Optional.empty();
        });
    }

    @Override
    public void deleteRequestCorrelation(String matsSocketSessionId,
            String serverMessageId) throws DataAccessException {
        withConnectionVoid(con -> {
            // Delete the Correlation
            PreparedStatement deleteCorrelation = con.prepareStatement("DELETE FROM " + requestOutTableName(
                    matsSocketSessionId)
                    + " WHERE session_id = ?"
                    + " AND smid = ?");
            deleteCorrelation.setString(1, matsSocketSessionId);
            deleteCorrelation.setString(2, serverMessageId);
            deleteCorrelation.execute();
        });
    }

    // ---------- Table name generators ----------

    private static int tableNumFromSessionId(String sessionIdForHash) {
        return Math.floorMod(sessionIdForHash.hashCode(), NUMBER_OF_BOX_TABLES);
    }

    private static String inboxTableName(String sessionIdForHash) {
        return inboxTableName(tableNumFromSessionId(sessionIdForHash));
    }

    private static String inboxTableName(int tableNum) {
        // Handle up to 100 tables ("00" - "99")
        return INBOX_TABLE_PREFIX + (tableNum < 10 ? "0" + tableNum : Integer.toString(tableNum));
    }

    private static String outboxTableName(String sessionIdForHash) {
        return outboxTableName(tableNumFromSessionId(sessionIdForHash));
    }

    private static String outboxTableName(int tableNum) {
        // Handle up to 100 tables ("00" - "99")
        return OUTBOX_TABLE_PREFIX + (tableNum < 10 ? "0" + tableNum : Integer.toString(tableNum));
    }

    private static String requestOutTableName(String sessionIdForHash) {
        return requestOutTableName(tableNumFromSessionId(sessionIdForHash));
    }

    private static String requestOutTableName(int tableNum) {
        // Handle up to 100 tables ("00" - "99")
        return REQUEST_OUT_TABLE_PREFIX + (tableNum < 10 ? "0" + tableNum : Integer.toString(tableNum));
    }

    // ==============================================================================
    // ==== DO NOT READ ANY CODE BELOW THIS POINT. It will just hurt your eyes. =====
    // ==============================================================================

    private <T> T withConnectionReturn(Lambda<T> lambda) throws DataAccessException {
        try {
            try (Connection con = _dataSource.getConnection()) {
                return lambda.transact(con);
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Got '" + e.getClass().getSimpleName() + "' accessing DataSource.", e);
        }
    }

    @FunctionalInterface
    private interface Lambda<T> {
        T transact(Connection con) throws SQLException;
    }

    private void withConnectionVoid(LambdaVoid lambdaVoid) throws DataAccessException {
        try {
            try (Connection con = _dataSource.getConnection()) {
                lambdaVoid.transact(con);
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Got '" + e.getClass().getSimpleName() + "' accessing DataSource.", e);
        }
    }

    @FunctionalInterface
    private interface LambdaVoid {
        void transact(Connection con) throws SQLException;
    }
}
