package com.stolsvik.mats.websocket.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endre Stølsvik 2019-12-08 11:00 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ClusterStoreAndForward_SQL implements ClusterStoreAndForward {
    private static final Logger log = LoggerFactory.getLogger(ClusterStoreAndForward_SQL.class);

    private final DataSource _dataSource;
    private final String _nodename;

    /**
     * Defines the String and Binary datatypes for some databases.
     */
    public enum Database {
        /**
         * <b>Default:</b> MS SQL: NVARCHAR(MAX), VARBINARY(MAX) (NOTE: H2 also handles these).
         */
        MS_SQL("NVARCHAR(MAX)", "VARBINARY(MAX)"),

        /**
         * MS SQL 2019 and above: VARCHAR(MAX) <b>(assumes UTF-8 collation type)</b>, VARBINARY(MAX) (NOTE: H2 also
         * handles these).
         */
        MS_SQL_2019("VARCHAR(MAX)", "VARBINARY(MAX)"),

        /**
         * H2: VARCHAR, VARBINARY (NOTE: H2 also handles {@link #MS_SQL} and {@link #MS_SQL_2019}).
         */
        H2("VARCHAR", "VARBINARY"),

        /**
         * PostgreSQL: TEXT, BYTEA
         */
        POSTGRESQL("TEXT", "BYTEA"),

        /**
         * Oracle: NCLOB, BLOB
         */
        ORACLE("NCLOB", "BLOB"),

        /**
         * MySQL / MariaDB: LONGTEXT, LONGBLOB (both 32 bits)
         */
        MYSQL("LONGTEXT", "LONGBLOB");

        private final String _textType;
        private final String _binaryType;

        Database(String textType, String binaryType) {
            _textType = textType;
            _binaryType = binaryType;
        }

        public String getTextType() {
            return _textType;
        }

        public String getBinaryType() {
            return _binaryType;
        }
    }

    /**
     * Use {@link Database#MS_SQL} types - H2 also handles these.
     */
    public static ClusterStoreAndForward_SQL create(DataSource dataSource, String nodename) {
        return new ClusterStoreAndForward_SQL(dataSource, nodename);
    }

    public static ClusterStoreAndForward_SQL create(DataSource dataSource, String nodename, Database database) {
        ClusterStoreAndForward_SQL csaf = new ClusterStoreAndForward_SQL(dataSource, nodename);
        csaf.setTextAndBinaryTypes(database);
        return csaf;
    }

    public static ClusterStoreAndForward_SQL create(DataSource dataSource, String nodename, String textType,
            String binaryType) {
        ClusterStoreAndForward_SQL csaf = new ClusterStoreAndForward_SQL(dataSource, nodename);
        csaf.setTextAndBinaryTypes(textType, binaryType);
        return csaf;
    }

    private ClusterStoreAndForward_SQL(DataSource dataSource, String nodename) {
        _dataSource = dataSource;
        _nodename = nodename;
    }

    // MS SQL and H2 handles this
    private String _textType = Database.MS_SQL.getTextType();
    private String _binaryType = Database.MS_SQL.getBinaryType();

    public void setTextAndBinaryTypes(Database database) {
        setTextAndBinaryTypes(database.getTextType(), database.getBinaryType());
    }

    public void setTextAndBinaryTypes(String textType, String binaryType) {
        _textType = textType;
        _binaryType = binaryType;
    }

    @Override
    public void boot() {
        HashMap<String, String> placeHolders = new HashMap<>();
        placeHolders.put("texttype", _textType);
        placeHolders.put("binarytype", _binaryType);

        String dbMigrationsLocation = ClusterStoreAndForward.class.getPackage().getName()
                .replace('.', '/')
                + '.' + "db_migrations";

        String dbMig = "/com/stolsvik/mats/websocket/impl/db_migrations";

        log.info("'db_migrations' location: " + dbMigrationsLocation);

        Flyway.configure().dataSource(_dataSource)
                .placeholders(placeHolders)
                .locations(dbMig)
                .load()
                .migrate();
    }

    @Override
    public void registerSessionAtThisNode(String matsSocketSessionId) throws DataAccessException {
        try {
            Connection con = _dataSource.getConnection();
            try {
                boolean autoCommitPre = con.getAutoCommit();
                try {
                    con.setAutoCommit(false);
                    PreparedStatement delete = con.prepareStatement("DELETE FROM mats_socket_message"
                            + " WHERE mats_session_id = ?");
                    delete.setString(1, matsSocketSessionId);
                    delete.execute();

                    PreparedStatement insert = con.prepareStatement("INSERT INTO mats_socket_session"
                            + "(mats_session_id, nodename, liveliness_timestamp)"
                            + "VALUES (?, ?, ?)");
                    insert.setString(1, matsSocketSessionId);
                    insert.setString(2, _nodename);
                    insert.setLong(3, System.currentTimeMillis());
                    insert.execute();
                    con.commit();
                }
                finally {
                    con.setAutoCommit(autoCommitPre);
                }
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    @Override
    public void deregisterSessionFromThisNode(String matsSocketSessionId) throws DataAccessException {
        try {
            Connection con = _dataSource.getConnection();
            try {
                // Note that we include a "WHERE nodename = us" here, so as to not mess up if he has already
                // re-registered
                PreparedStatement update = con.prepareStatement("UPDATE mats_socket_session"
                        + "   SET nodename = NULL"
                        + " WHERE mats_session_id = ?"
                        + "   AND nodename = ?");
                update.setString(1, matsSocketSessionId);
                update.setString(2, _nodename);
                update.execute();
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    @Override
    public Optional<String> getCurrentNodeForSession(String matsSocketSessionId) throws DataAccessException {
        try {
            Connection con = _dataSource.getConnection();
            try {
                return currentNodeForSession(matsSocketSessionId, con);
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    private Optional<String> currentNodeForSession(String matsSocketSessionId, Connection con) throws SQLException {
        PreparedStatement select = con.prepareStatement("SELECT nodename FROM mats_socket_session"
                + " WHERE mats_session_id = ?");
        select.setString(1, matsSocketSessionId);
        ResultSet resultSet = select.executeQuery();
        boolean next = resultSet.next();
        if (!next) {
            return Optional.empty();
        }
        String nodename = resultSet.getString(1);
        return Optional.of(nodename);
    }

    @Override
    public void notifySessionLiveliness(List<String> matsSocketSessionIds) throws DataAccessException {
        long now = System.currentTimeMillis();
        try {
            Connection con = _dataSource.getConnection();
            try {
                // TODO / OPTIMIZE: Make "in" and "batch" optimizations.
                for (String matsSocketSessionId : matsSocketSessionIds) {

                    PreparedStatement update = con.prepareStatement("UPDATE mats_socket_session"
                            + "   SET liveliness_timestamp = ?"
                            + " WHERE mats_session_id = ?");
                    update.setLong(1, now);
                    update.setString(2, matsSocketSessionId);
                    update.execute();
                }
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    @Override
    public void terminateSession(String matsSocketSessionId) throws DataAccessException {
        try {
            Connection con = _dataSource.getConnection();
            try {
                // Notice that we DO NOT include WHERE nodename is us. User asked us to delete, and that we do.
                PreparedStatement deleteSession = con.prepareStatement("DELETE FROM mats_socket_session"
                        + " WHERE mats_session_id = ?");
                deleteSession.setString(1, matsSocketSessionId);
                deleteSession.execute();

                PreparedStatement deleteMessages = con.prepareStatement("DELETE FROM mats_socket_message"
                        + " WHERE mats_session_id = ?");
                deleteMessages.setString(1, matsSocketSessionId);
                deleteMessages.execute();
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    @Override
    public Optional<String> storeMessageForSession(String matsSocketSessionId, String traceId, String type,
            String message) throws DataAccessException {

        try {
            Connection con = _dataSource.getConnection();
            try {
                PreparedStatement insert = con.prepareStatement("INSERT INTO mats_socket_message"
                        + "(message_id, mats_session_id, trace_id,"
                        + " stored_timestamp, delivery_count, type, message_text)"
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)");
                long randomId = ThreadLocalRandom.current().nextLong();
                insert.setLong(1, randomId);
                insert.setString(2, matsSocketSessionId);
                insert.setString(3, traceId);
                insert.setLong(4, System.currentTimeMillis());
                insert.setInt(5, 0);
                insert.setString(6, type);
                insert.setString(7, message);
                insert.execute();

                return currentNodeForSession(matsSocketSessionId, con);
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    @Override
    public List<StoredMessage> getMessagesForSession(String matsSocketSessionId, int maxNumberOfMessages) throws DataAccessException {
        try {
            Connection con = _dataSource.getConnection();
            try {
                // The old MS JDBC Driver 'jtds' don't handle parameter insertion for TOP.
                PreparedStatement insert = con.prepareStatement("SELECT TOP " + maxNumberOfMessages
                        + "          message_id, mats_session_id, trace_id,"
                        + "          stored_timestamp, delivery_count, type, message_text"
                        + "  FROM mats_socket_message"
                        + " WHERE mats_session_id = ?");
                insert.setString(1, matsSocketSessionId);
                ResultSet rs = insert.executeQuery();
                List<StoredMessage> list = new ArrayList<>();
                while (rs.next()) {
                    StoredMessageImpl sm = new StoredMessageImpl(rs.getLong(1), rs.getInt(5),
                            rs.getLong(4), rs.getString(6), rs.getString(3),
                            rs.getString(7));
                    list.add(sm);
                }
                return list;
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    @Override
    public void messagesComplete(String matsSocketSessionId, List<Long> messageIds) throws DataAccessException {
        try {
            Connection con = _dataSource.getConnection();
            try {
                // TODO / OPTIMIZE: Make "in" and "batch" optimizations.
                for (Long messageId : messageIds) {

                    PreparedStatement deleteMsg = con.prepareStatement("DELETE FROM mats_socket_message"
                            + " WHERE mats_session_id = ?"
                            + "   AND message_id = ?");
                    deleteMsg.setString(1, matsSocketSessionId);
                    deleteMsg.setLong(2, messageId);
                    deleteMsg.execute();
                }
            }
            finally {
                con.close();
            }
        }
        catch (SQLException e) {
            throw new DataAccessException("Problems accessing DataSource.", e);
        }
    }

    @Override
    public void messagesFailedDelivery(String matsSocketSessionId, List<Long> messageIds) {

    }
}
