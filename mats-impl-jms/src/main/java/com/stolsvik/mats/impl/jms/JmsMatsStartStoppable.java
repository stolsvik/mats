package com.stolsvik.mats.impl.jms;

import com.stolsvik.mats.MatsConfig.StartStoppable;

import java.util.List;

/**
 * @author Endre Stølsvik 2019-08-23 23:30 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface JmsMatsStartStoppable extends JmsMatsStatics, StartStoppable {

    List<JmsMatsStartStoppable> getChildrenStartStoppable();

    @Override
    default boolean waitForStarted(int timeoutMillis) {
        int millisLeft = timeoutMillis;
        boolean started = true;
        for (JmsMatsStartStoppable child : getChildrenStartStoppable()) {
            long millisBefore = System.currentTimeMillis();
            started &= child.waitForStarted(millisLeft);
            millisLeft -= (System.currentTimeMillis() - millisBefore);
            millisLeft = millisLeft < EXTRA_GRACE_MILLIS ? EXTRA_GRACE_MILLIS : millisLeft;
        }
        return started;
    }


    default void stopPhase0SetRunFlagFalseAndCloseSessionIfInReceive() {
        getChildrenStartStoppable().forEach(JmsMatsStartStoppable::stopPhase0SetRunFlagFalseAndCloseSessionIfInReceive);
    }

    default void stopPhase1GracefulWait(int gracefulShutdownMillis) {
        int millisLeft = gracefulShutdownMillis;
        for (JmsMatsStartStoppable child : getChildrenStartStoppable()) {
            long millisBefore = System.currentTimeMillis();
            child.stopPhase1GracefulWait(millisLeft);
            millisLeft -= (System.currentTimeMillis() - millisBefore);
            millisLeft = millisLeft < EXTRA_GRACE_MILLIS ? EXTRA_GRACE_MILLIS : millisLeft;
        }
    }

    default void stopPhase2InterruptIfStillAlive() {
        getChildrenStartStoppable().forEach(JmsMatsStartStoppable::stopPhase2InterruptIfStillAlive);
    }

    default boolean stopPhase3GracefulAfterInterrupt() {
        boolean started = true;
        for (JmsMatsStartStoppable child : getChildrenStartStoppable()) {
            started &= child.stopPhase3GracefulAfterInterrupt();
        }
        return started;
    }

    @Override
    default boolean stop(int gracefulShutdownMillis) {
        stopPhase0SetRunFlagFalseAndCloseSessionIfInReceive();
        stopPhase1GracefulWait(gracefulShutdownMillis);
        stopPhase2InterruptIfStillAlive();
        return stopPhase3GracefulAfterInterrupt();
    }
}
