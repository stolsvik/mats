package com.stolsvik.mats.impl.jms;

import java.sql.Connection;
import java.util.Optional;
import java.util.function.Supplier;

import javax.jms.MessageConsumer;

import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;

/**
 * This is an internal context object, for the processing in JMS-MATS. This is used to communicate back and forth
 * between JmsMats proper, the {@link JmsMatsJmsSessionHandler} and {@link JmsMatsTransactionManager}.
 *
 * @author Endre Stølsvik 2019-08-11 23:04 - http://stolsvik.com/, endre@stolsvik.com
 */
public class JmsMatsMessageContext {

    private final JmsSessionHolder _jmsSessionHolder;
    private final MessageConsumer _messageConsumer;

    public JmsMatsMessageContext(JmsSessionHolder jmsSessionHolder, MessageConsumer messageConsumer) {
        _jmsSessionHolder = jmsSessionHolder;
        _messageConsumer = messageConsumer;
    }

    public JmsSessionHolder getJmsSessionHolder() {
        return _jmsSessionHolder;
    }

    /**
     * @return the {@link MessageConsumer} in effect if this is in a {@link JmsMatsStageProcessor} and not in a
     *         {@link JmsMatsInitiator}.
     */
    public Optional<MessageConsumer> getMessageConsumer() {
        return Optional.ofNullable(_messageConsumer);
    }

    private Supplier<Connection> _sqlConnectionSupplier;

    /**
     * Set supplier of SQL Connection if {@link JmsMatsTransactionManager} handles SQL Connections.
     */
    public void setSqlConnectionSupplier(Supplier<Connection> sqlConnectionSupplier) {
        _sqlConnectionSupplier = sqlConnectionSupplier;
    }

    /**
     * Employed by JmsMatsProcessContext.getAttribute(Connection.class) invocations.
     */
    public Optional<Connection> getSqlConnection() {
        return _sqlConnectionSupplier == null
                ? Optional.empty()
                : Optional.of(_sqlConnectionSupplier.get());
    }

    private Supplier<Boolean> _sqlConnectionEmployed;

    /**
     * The {@link JmsMatsTransactionManager} should set a way to determine whether the SQL Connection was actually
     * employed (if this is not possible to determine, then return whether it was gotten).
     */
    public void setSqllConnectionEmployed(Supplier<Boolean> sqlConnectionEmployed) {
        _sqlConnectionEmployed = sqlConnectionEmployed;
    }

    /**
     * Used by JMS Mats to point out whether the SQL Connection was actually employed (if this is not possible to
     * determine, then return whether it was gotten).
     *
     * @return true if the SQL Connection was actually employed (if this is not possible to determine, then return
     *         whether it was gotten).
     */
    boolean wasSqlConnectionEmployed() {
        return _sqlConnectionEmployed == null
                ? false
                : _sqlConnectionEmployed.get();
    }
}
