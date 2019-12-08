package com.stolsvik.mats.websocket.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

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
         * H2: VARCHAR, VARBINARY (NOTE: H2 also handles {@link #MS_SQL})
         */
        H2("VARCHAR", "VARBINARY"),

        /**
         * MS SQL: VARCHAR(MAX), VARBINARY(MAX) NOTE: H2 also handles these.
         */
        MS_SQL("VARCHAR(MAX)", "VARBINARY(MAX)"),

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
     * Use MS SQL types - H2 also handles these.
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
    public void registerSessionAtThisNode(String matsSocketSessionId) {

    }

    @Override
    public void deregisterSessionFromThisNode(String matsSocketSessionId) {

    }

    @Override
    public Optional<String> getCurrentNodeForSession(String matsSocketSessionId) {
        return Optional.empty();
    }

    @Override
    public void notifySessionLiveliness(List<String> matsSocketSessionIds) {

    }

    @Override
    public void terminateSession(String matsSocketSessionId) {

    }

    @Override
    public Optional<String> storeMessageForSession(String matsSocketSessionId, String traceId, String type,
            String message) {
        return Optional.empty();
    }

    @Override
    public List<StoredMessage> getMessagesForSession(String matsSocketSessionId, int maxNumberOfMessages) {
        return null;
    }

    @Override
    public void messagesComplete(String matsSocketSessionId, List<Long> messageIds) {

    }

    @Override
    public void messagesFailedDelivery(String matsSocketSessionId, List<Long> messageIds) {

    }
}
