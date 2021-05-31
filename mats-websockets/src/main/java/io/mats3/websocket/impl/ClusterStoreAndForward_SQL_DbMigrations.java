package io.mats3.websocket.impl;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.websocket.ClusterStoreAndForward;

/**
 * Perform the DB-setup/migrations that {@link ClusterStoreAndForward_SQL} needs, using Flyway. These needs to be
 * run before the CSAF_SQL is booted up.
 *
 * @author Endre Stølsvik 2020-02-23 12:36 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ClusterStoreAndForward_SQL_DbMigrations {
    private static final Logger log = LoggerFactory.getLogger(ClusterStoreAndForward_SQL_DbMigrations.class);

    /**
     * Defines the String and Binary datatypes for some databases.
     */
    public enum Database {
        /**
         * <b>Default:</b> MS SQL: NVARCHAR(MAX), VARBINARY(MAX) (NOTE: H2 also handles these).
         */
        MS_SQL("NVARCHAR(MAX)", "VARBINARY(MAX)"),

        /**
         * MS SQL 2019 and above: <b>(assumes UTF-8 collation type)</b> VARCHAR(MAX), VARBINARY(MAX) (NOTE: H2 also
         * handles these).
         */
        MS_SQL_2019_UTF8("VARCHAR(MAX)", "VARBINARY(MAX)"),

        /**
         * H2: VARCHAR, VARBINARY (NOTE: H2 also handles {@link #MS_SQL} and {@link #MS_SQL_2019_UTF8}).
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

    public static ClusterStoreAndForward_SQL_DbMigrations create(Database database) {
        ClusterStoreAndForward_SQL_DbMigrations csaf = new ClusterStoreAndForward_SQL_DbMigrations();
        csaf.setTextAndBinaryTypes(database);
        return csaf;
    }

    public static ClusterStoreAndForward_SQL_DbMigrations create(String textType, String binaryType) {
        ClusterStoreAndForward_SQL_DbMigrations csaf = new ClusterStoreAndForward_SQL_DbMigrations();
        csaf.setTextAndBinaryTypes(textType, binaryType);
        return csaf;
    }

    protected ClusterStoreAndForward_SQL_DbMigrations() {
        /* no-op */
    }

    protected String _textType;
    protected String _binaryType;

    public void setTextAndBinaryTypes(Database database) {
        setTextAndBinaryTypes(database.getTextType(), database.getBinaryType());
    }

    public void setTextAndBinaryTypes(String textType, String binaryType) {
        _textType = textType;
        _binaryType = binaryType;
    }

    /**
     * @return the placeholders key-value pairs for the two placeholders needed for the migration scripts, "texttype"
     *         and "binarytype".
     */
    public Map<String, String> getPlaceHolders() {
        HashMap<String, String> placeHolders = new HashMap<>();
        placeHolders.put("texttype", _textType);
        placeHolders.put("binarytype", _binaryType);
        return placeHolders;
    }

    /**
     * @return the classpath location of the migrations scripts, <b>note that you need two placeholders in these
     *         scripts</b> ("texttype" and "binarytype"), gotten by {@link #getPlaceHolders()}.
     */
    public String getMigrationsLocation() {
        return ClusterStoreAndForward_SQL_DbMigrations.class.getPackage().getName()
                .replace('.', '/') + '/' + "db_migrations";
    }

    /**
     * Perform migrations using Flyway, getting the placeholder (interpolated values for "texttype" and "binarytype")
     * from {@link #getPlaceHolders()}, and the migration scrips from {@link #getMigrationsLocation()}.
     */
    public void migrateUsingFlyway(DataSource dataSource) {
        String dbMigrationsLocation = getMigrationsLocation();

        log.info("'db_migrations' location: " + dbMigrationsLocation);

        Flyway.configure().dataSource(dataSource)
                .placeholders(getPlaceHolders())
                .locations(dbMigrationsLocation)
                .load()
                .migrate();
    }
}
