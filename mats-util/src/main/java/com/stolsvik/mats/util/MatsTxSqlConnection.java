package com.stolsvik.mats.util;

import java.sql.Connection;
import java.util.function.Supplier;

import javax.sql.DataSource;

import com.stolsvik.mats.MatsStage;
import com.stolsvik.mats.exceptions.MatsRuntimeException;

/**
 * The different MATS Transaction Managers that include and handle a SQL {@link Connection} will provide some means for
 * {@link MatsStage} Stage Processors to get hold of the transactional Connection by using the static method
 * {@link #getConnection()}. This is done by the MATS Transaction Manager binding a SQL Connection Supplier to a
 * ThreadLocal by invoking the {@link #setThreadLocalConnectionSupplier(Supplier)}.
 * <p>
 * Notice that within a Spring environment, the usual way to get hold of the SQL Connection is by means of an injected
 * {@link DataSource} which the Spring transaction system has taken control of, so that the Connection you get out of
 * that DataSource is the transactional Connection for that thread. There is however no similar logic outside of such a
 * IoC Container, so this is class is made to be a portable way for a MATS Stage Processor to get a SQL Connection: It
 * shall work both outside the IoC container ("pure java"), and within the IoC container (DataSource proxying by the
 * Spring Transaction Manager in conjunction with the IoC system).
 *
 * @author Endre Stølsvik - 2015-12-05 - http://endre.stolsvik.com
 */
public final class MatsTxSqlConnection {
    static ThreadLocal<Supplier<Connection>> __threadLocalConnectionSupplier = new ThreadLocal<>();

    /**
     * Invoked by the MATS Transaction Manager which is in effect for the current {@link MatsStage}'s Stage Processor
     * Thread, providing a means to get hold of the transactional SQL Connection.
     *
     * @param connectionSupplier
     *            the ThreadLocal {@link Supplier} of {@link Connection}, or <code>null</code> to clear the ThreadLocal.
     */
    public static void setThreadLocalConnectionSupplier(Supplier<Connection> connectionSupplier) {
        __threadLocalConnectionSupplier.set(connectionSupplier);
    }

    /**
     * A {@link MatsRuntimeException} that should be raised by the {@literal Supplier<Connection>} if it can't get SQL
     * Connections.
     */
    public static class MatsSqlConnectionCreationException extends MatsRuntimeException {
        public MatsSqlConnectionCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * @return the SQL Connection bound to the current {@link MatsStage}'s Stage Processor.
     */
    public static Connection getConnection() {
        Supplier<Connection> supplier = __threadLocalConnectionSupplier.get();
        if (supplier == null) {
            throw new IllegalStateException("There is evidently no Transactional SQL Connection Supplier available:"
                    + " Either you are not within a MatsStage's Stage Processor, or the current MATS Transaction"
                    + " Manager does not hold SQL Connections.");
        }
        return supplier.get();
    }
}
