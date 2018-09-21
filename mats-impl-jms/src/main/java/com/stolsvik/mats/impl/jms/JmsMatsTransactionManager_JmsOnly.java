package com.stolsvik.mats.impl.jms;

import javax.jms.JMSException;
import javax.jms.Session;

import com.stolsvik.mats.impl.jms.JmsMatsJmsSessionHandler.JmsSessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stolsvik.mats.exceptions.MatsBackendException;
import com.stolsvik.mats.exceptions.MatsRefuseMessageException;
import com.stolsvik.mats.exceptions.MatsRuntimeException;
import com.stolsvik.mats.impl.jms.JmsMatsStage.JmsMatsStageProcessor;

/**
 * Implementation of {@link JmsMatsTransactionManager} handling only JMS (getting Connections, and creating Sessions),
 * doing all transactional handling "native", i.e. using only the JMS API (as opposed to e.g. using Spring and its
 * transaction managers).
 * <p>
 * This {@link JmsMatsTransactionManager} (currently) creates a new JMS Connection per invocation to
 * {@link #getTransactionContext(JmsMatsStage)}, implying that each {@link JmsMatsStage} and each
 * {@link JmsMatsInitiator} gets its own JMS Connection, while each {@link JmsMatsStageProcessor} of a stage shares the
 * stage's JMS Connection (due to it invoking {@link TransactionalContext_JmsOnly#getTransactionalJmsSession(boolean)}).
 *
 * @author Endre Stølsvik - 2015 - http://endre.stolsvik.com
 */
public class JmsMatsTransactionManager_JmsOnly implements JmsMatsTransactionManager, JmsMatsStatics {
    private static final Logger log = LoggerFactory.getLogger(JmsMatsTransactionManager_JmsOnly.class);

    public static JmsMatsTransactionManager create() {
        return new JmsMatsTransactionManager_JmsOnly();
    }

    protected JmsMatsTransactionManager_JmsOnly() {
        /* hide; use factory method */
    }

    @Override
    public TransactionContext getTransactionContext(JmsMatsTxContextKey txContextKey) {
        return new TransactionalContext_JmsOnly(txContextKey);
    }

    /**
     * The {@link JmsMatsTransactionManager.TransactionContext} implementation for
     * {@link JmsMatsTransactionManager_JmsOnly}.
     */
    public static class TransactionalContext_JmsOnly implements TransactionContext, JmsMatsStatics {

        protected final JmsMatsTxContextKey _txContextKey;

        public TransactionalContext_JmsOnly(JmsMatsTxContextKey txContextKey) {
            _txContextKey = txContextKey;
        }

        @Override
        public void doTransaction(JmsSessionHolder jmsSessionHolder, ProcessingLambda lambda) throws JMSException {

            /*
             * We're always within a JMS transaction (as that is the nature of the JMS API when in transactional mode).
             *
             * -- Therefore, we're now *within* the JMS Transaction demarcation.
             */

            Session jmsSession = jmsSessionHolder.getSession();

            try {
                log.debug(LOG_PREFIX + "About to run ProcessingLambda for " + stageOrInit(_txContextKey)
                        + ", within JMS Transactional demarcation.");
                /*
                 * Invoking the provided ProcessingLambda, which typically will be the actual user code (albeit wrapped
                 * with some minor code from the JmsMatsStage to parse the MapMessage, deserialize the MatsTrace, and
                 * fetch the state etc.), which will now be inside both the inner (implicit) SQL Transaction
                 * demarcation, and the outer JMS Transaction demarcation.
                 */
                lambda.performWithinTransaction();
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
                log.error(LOG_PREFIX + "ROLLBACK JMS: Got a " + MatsRefuseMessageException.class.getSimpleName() +
                        " while transacting " + stageOrInit(_txContextKey) + " (most probably from the user code)."
                        + " Rolling back the JMS transaction - trying to ensure that it goes directly to DLQ.", e);
                JmsMatsActiveMQSpecifics.instaDlq(jmsSession, () -> rollback(jmsSession, e));
                // Return nicely, going into .receive() again.
                return;
            }
            catch (MatsBackendException e) {
                /*
                 * This denotes that the JmsMatsProcessContext (the JMS Mats implementation - i.e. us) has had problems
                 * doing JMS stuff. The JMSException will be wrapped in this MatsBackendException as the MATS API is
                 * implementation agnostic, and there is no mention of the JMS API in it. This might happen on any of
                 * the JMS interaction points, i.e. on initiate, or within a stage when requesting, replying, or adding
                 * binaries or strings. Sending this on to the outside catch block, as this means that we have an
                 * unstable JMS context.
                 */
                log.error(LOG_PREFIX + "ROLLBACK JMS: Got a " + MatsBackendException.class.getSimpleName()
                        + " while transacting " + stageOrInit(_txContextKey)
                        + ", indicating that the MATS JMS implementation had problems performing"
                        + " some operation. Rolling back JMS Session, throwing on to get new JMS Connection.", e);
                rollback(jmsSession, e);
                // Throwing out, since the JMS Connection most probably is unstable.
                throw e;
            }
            catch (RuntimeException | Error e) {
                /*
                 * Should only be user code, as errors from "ourselves" (the JMS MATS impl) should throw
                 * MatsBackendException, and are caught earlier (see above).
                 */
                log.error(LOG_PREFIX + "ROLLBACK JMS: " + e.getClass().getSimpleName() + " while transacting "
                        + stageOrInit(_txContextKey) + " (should only be from user code)."
                        + " Rolling back the JMS session.", e);
                rollback(jmsSession, e);
                // Throw on, so that if this is in an initiate-call, it will percolate all the way out.
                // (Inside JmsMatsStage, RuntimeExceptions won't recreate the JMS Connection..)
                throw e;
            }
            catch (Throwable t) {
                /*
                 * This must have been a "sneaky throws"; Throwing of an undeclared checked exception.
                 */
                log.error(LOG_PREFIX + "ROLLBACK JMS: " + t.getClass().getSimpleName() + " while transacting "
                        + stageOrInit(_txContextKey) + " (probably 'sneaky throws' of checked exception)."
                        + " Rolling back the JMS session.", t);
                rollback(jmsSession, t);
                // Throw on, so that if this is in an initiate-call, it will percolate all the way out.
                // (Inside JmsMatsStage, RuntimeExceptions won't recreate the JMS Connection..)
                throw new MatsRuntimeException("Got a undeclared checked exception " + t.getClass().getSimpleName()
                        + " while transacting " + stageOrInit(_txContextKey) + ".", t);
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
                        + " commit the JMS Session! If this happened within a MATS message initiation, the state"
                        + " changing operations (e.g. database insert/update) have been committed, while the message"
                        + " was not sent. If this is not caught by the initiation code ('manually' rolling back the"
                        + " state change), the global state is probably out of sync (i.e. the order-row is marked"
                        + " 'processing started', while the corresponding process-order message was not sent). However,"
                        + " if this happened within a MATS Stage (inside an endpoint), this will most probably just"
                        + " lead to a redelivery (as in 'double delivery'), which should be handled just fine by your"
                        + " idempotent code..!",
                        t);
                /*
                 * This certainly calls for reestablishing the JMS Session, so throw out.
                 */
                throw new MatsBackendException("VERY BAD! After finished transacting " + stageOrInit(_txContextKey)
                        + ", we could not commit JMS Session!", t);
            }

            // -> The JMS Session nicely committed.
            log.debug(LOG_PREFIX + "JMS Session committed.");
        }

        private void rollback(Session jmsSession, Throwable t) {
            try {
                jmsSession.rollback();
                // -> The JMS Session rolled nicely back.
                log.warn(LOG_PREFIX + "JMS Session rolled back.");
            }
            catch (Throwable rollbackT) {
                /*
                 * Could not roll back. This certainly indicates that we have some problem with the JMS Session, and
                 * we'll throw it out so that we start a new JMS Session.
                 *
                 * However, it is not that bad, as the JMS Message Broker probably will redeliver anyway.
                 */
                throw new MatsBackendException("When trying to rollback JMS Session due to a "
                        + t.getClass().getSimpleName()
                        + ", we got some Exception. The JMS Session certainly seems unstable.", rollbackT);
            }
        }
    }
}
