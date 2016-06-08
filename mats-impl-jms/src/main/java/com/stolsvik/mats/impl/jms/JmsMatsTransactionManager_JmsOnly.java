package com.stolsvik.mats.impl.jms;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.exceptions.MatsConnectionException;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;
import com.stolsvik.mats.impl.jms.JmsMatsStage.StageProcessor;

/**
 * Implementation of {@link JmsMatsTransactionManager} handling only JMS (getting Connections, and creating Sessions),
 * doing all transactional handling "native", i.e. using only the JMS API (as opposed to e.g. using Spring and its
 * transaction managers).
 * <p>
 * This {@link JmsMatsTransactionManager} (currently) creates a new JMS Connection per invocation to
 * {@link #getTransactionContext(JmsMatsStage)}, implying that each {@link JmsMatsStage} and each
 * {@link JmsMatsInitiator} gets its own JMS Connection, while each {@link StageProcessor} of a stage shares the stage's
 * JMS Connection (due to it invoking {@link TransactionalContext_JmsOnly#getTransactionalJmsSession(boolean)}).
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsTransactionManager_JmsOnly implements JmsMatsTransactionManager, JmsMatsStatics {

    private static final Logger log = LoggerFactory.getLogger(JmsMatsTransactionManager_JmsOnly.class);

    private final JmsConnectionSupplier _jmsConnectionSupplier;

    public static final JmsMatsTransactionManager create(JmsConnectionSupplier jmsConnectionSupplier) {
        return new JmsMatsTransactionManager_JmsOnly(jmsConnectionSupplier);
    }

    protected JmsMatsTransactionManager_JmsOnly(JmsConnectionSupplier jmsConnectionSupplier) {
        _jmsConnectionSupplier = jmsConnectionSupplier;
    }

    @Override
    public TransactionContext getTransactionContext(JmsMatsStage<?, ?, ?> stage) {
        return new TransactionalContext_JmsOnly(createJmsConnection(stage), stage);
    }

    /**
     * @return a new JMS {@link Connection}.
     */
    protected Connection createJmsConnection(JmsMatsStage<?, ?, ?> stage) {
        try {
            Connection jmsConnection = _jmsConnectionSupplier.createJmsConnection(stage);
            // Starting it right away, as that could potentially also give "connection establishment" JMSExceptions
            jmsConnection.start();
            return jmsConnection;
        }
        catch (JMSException e) {
            throw new MatsConnectionException("Got a JMS Exception when trying to createConnection()"
                    + " on the JmsConnectionSupplier [" + _jmsConnectionSupplier + "].", e);
        }
    }

    /**
     * The {@link JmsMatsTransactionManager.TransactionContext} implementation for
     * {@link JmsMatsTransactionManager_JmsOnly}.
     */
    public static class TransactionalContext_JmsOnly implements TransactionContext, JmsMatsStatics {

        private final Connection _jmsConnection;
        private final JmsMatsStage<?, ?, ?> _stage;

        public TransactionalContext_JmsOnly(Connection jmsConnection, JmsMatsStage<?, ?, ?> stage) {
            _jmsConnection = jmsConnection;
            _stage = stage;
        }

        @Override
        public Session getTransactionalJmsSession(boolean canBlock) {
            try {
                return _jmsConnection.createSession(true, 0);
            }
            catch (JMSException e) {
                throw new MatsBackendException("Got a JMS Exception when trying to createSession(true, 0)"
                        + " on the JMS Connection [" + _jmsConnection + "].", e);
            }
        }

        @Override
        public void performWithinTransaction(Session jmsSession, ProcessingLambda lambda) throws JMSException {

            /*
             * We're always within a JMS transaction (as that is the nature of the JMS API when in transactional modus).
             *
             * -- Therefore, we're now *within* the JMS Transaction demarcation.
             */

            // Flag to check that we've handled all paths we know of.
            boolean allPathsHandled = false;
            try {
                log.debug(LOG_PREFIX + "About to run ProcessingLambda for " + stageOrInit(_stage)
                        + ", within JMS Transactional demarcation.");
                /*
                 * Invoking the provided ProcessingLambda, which typically will be the actual user code (albeit wrapped
                 * with some minor code from the JmsMatsStage to parse the MapMessage, deserialize the MatsTrace, and
                 * fetch the state etc.), which will now be inside both the inner (implicit) SQL Transaction
                 * demarcation, and the outer JMS Transaction demarcation.
                 */

                lambda.performWithinTransaction();

                // ProcessingLambda went OK: "Good path" is handled.
                allPathsHandled = true;
            }
            /*
             * Catch EVERYTHING that can come out of the try-block, with some cases handled specially.
             */
            catch (MatsRefuseMessageException e) {
                /*
                 * Special exception allowed from the MATS API from the MatsStage lambda, denoting that one wants
                 * immediate refusal of the message. (This is just a hint/wish, as e.g. the JMS specification does
                 * provide such a mechanism).
                 */
                // The ProcessngLambda raised some Exception, and we're handling: A "Bad path" is handled.
                allPathsHandled = true;

                log.error(LOG_PREFIX + "ROLLBACK JMS: Got a " + MatsRefuseMessageException.class.getSimpleName() +
                        " while processing " + stageOrInit(_stage) + " (most probably from the user code)."
                        + " Rolling back the JMS transaction - trying to ensure that it goes directly to DLQ.", e);
                // TODO: Make ActiveMQ directly to DLQ stuff
                rollback(jmsSession, e);
                // -> The JMS Session rolled nicely back.
                log.info(LOG_PREFIX + "JMS Session rolled back.");
                return;
            }
            catch (MatsBackendException e) {
                /*
                 * This denotes that the ProcessContext (the JMS Mats implementation - i.e. us) has had problems doing
                 * JMS stuff. The JMSException will be wrapped in this MatsBackendException as the MATS API is
                 * implementation agnostic, and there is no mention of the JMS API in it. This might happen on any of
                 * the JMS interaction points, i.e. on initiate, or within a stage when requesting, replying, or adding
                 * binaries or strings. Sending this on to the outside catch block, as this means that we have an
                 * unstable JMS context.
                 */
                // The ProcessngLambda raised some Exception, and we're handling: A "Bad path" is handled.
                allPathsHandled = true;

                log.error(LOG_PREFIX + "ROLLBACK JMS: Got a " + MatsBackendException.class.getSimpleName()
                        + " while processing " + stageOrInit(_stage)
                        + ", indicating that the MATS JMS implementation had problems performing"
                        + " some operation. Rolling back JMS Session.", e);
                rollback(jmsSession, e);
                throw e;
            }
            catch (RuntimeException | Error e) {
                /*
                 * Should only be user code, as errors from "ourselves" (the JMS MATS impl) should throw
                 * MatsBackendExcetion, and are caught earlier (see above).
                 */
                // The ProcessngLambda raised some Exception, and we're handling: A "Bad path" is handled.
                allPathsHandled = true;

                log.error(LOG_PREFIX + "ROLLBACK JMS: " + e.getClass().getSimpleName() + " while processing "
                        + stageOrInit(_stage) + " (should only be from user code)."
                        + " Rolling back the JMS session.", e);
                rollback(jmsSession, e);
                // -> The JMS Session rolled nicely back.
                log.info(LOG_PREFIX + "JMS Session rolled back.");
                throw e;
            }
            // :: Sanity check that we have handled all paths
            finally {
                // ?: Are all paths handled?
                if (!allPathsHandled) {
                    // -> No: This is a MATS coding error, the catch block above should have caught the Exception.
                    String msg = "The " + stageOrInit(_stage)
                            + " raised some Exception that should have been caught! This should never happen!";
                    log.error(msg);
                    MatsBackendException e = new MatsBackendException(msg);
                    rollback(jmsSession, e);
                    throw e;
                }
            }

            // ----- The ProcessingLambda went OK, no Exception was raised.

            log.debug(LOG_PREFIX + "COMMIT JMS: ProcessingLambda finished, committing JMS Session.");
            try {
                jmsSession.commit();
            }
            catch (Throwable t) {
                /*
                 * WARNING WARNING! COULD NOT COMMIT JMS! Besides indicating that we have a JMS problem, we also have a
                 * potential bad situation with potentially committed external state changes, but where the JMS Message
                 * Broker cannot record our consumption of the message, and will probably have to (wrongly) redeliver
                 * it.
                 */
                log.error(LOG_PREFIX + "VERY BAD! After a MatsStage ProcessingLambda finished nicely, implying that"
                        + " any external, potentially state changing operations have committed OK, we could not"
                        + " commit the JMS Session! This will most probably lead to a redelivery (as in"
                        + " 'double delivery'), and the situation and any touched data should be inspected closely.",
                        t);
                /*
                 * This certainly calls for reestablishing the JMS Session, so throw out.
                 */
                throw new MatsBackendException("VERY BAD! After finished MatsStage Processing Lambda,"
                        + " we could not commit JMS Session!", t);
            }

            // -> The JMS Session nicely committed.
            log.debug(LOG_PREFIX + "JMS Session committed.");
        }

        private void rollback(Session jmsSession, Throwable t) {
            try {
                jmsSession.rollback();
            }
            catch (Throwable rollbackT) {
                /*
                 * Could not roll back. This certainly indicates that we have some problem with the JMS Session, and
                 * we'll throw it out so that we start new JMS Session.
                 *
                 * However, it is not that bad, as the JMS Message Broker probably will redeliver anyway.
                 */
                throw new MatsBackendException("When trying to rollback JMS Session due to a "
                        + t.getClass().getSimpleName()
                        + ", we got some Exception. The JMS Session certainly seems unstable.", rollbackT);
            }
        }

        @Override
        public void close() {
            Connection jmsConnection = _jmsConnection;
            if (jmsConnection != null) {
                try {
                    jmsConnection.close();
                }
                catch (Throwable t) {
                    log.error(LOG_PREFIX + "Got some unexpected exception when trying to close JMS Connection ["
                            + jmsConnection + "].", t);
                }
            }
        }
    }
}
